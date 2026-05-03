from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from bench_catalog import build_statement, load_sql_for_query, select_queries
from bench_common import (
    ConnOpts,
    OUTPUTS_DIR,
    QueryMeta,
    ResolvedDatasetRun,
    Scenario,
    Variant,
    die,
    safe_artifact_name,
    utc_now,
)
from bench_exec import (
    StatementTimeoutError,
    ensure_databases_reachable,
    resolved_variant_session_gucs,
    run_one,
    stabilize_db,
    validate_required_gucs,
)
from bench_results import build_run_context, write_raw_csv, write_run_context, write_summary_csv


MEASURED_REPS = 3
WARMUP_RUNS = 1


def run_scenario(
    scenario: Scenario,
    variants_registry: dict[str, Variant],
    variant_names: tuple[str, ...],
    resolved_runs: list[ResolvedDatasetRun],
    *,
    conn: Optional[ConnOpts],
    statement_timeout_ms: int,
    resume_run_id: Optional[str],
    tag: str,
) -> None:
    """Execute one resolved benchmark scenario and maintain resumable artifacts.

    A run is checkpointed only after whole query groups complete.  That keeps
    ``raw.csv``, ``summary.csv``, and ``run.json`` consistent enough for
    ``--resume-run-id`` to rebuild in-memory progress without replaying partial
    groups.  Fresh runs stabilize each prepared database before any query is
    executed; resumed runs reuse the existing statistics snapshot recorded by
    the partial artifact.  ``statement_timeout`` is a recorded benchmark result;
    non-timeout errors terminate the run after current artifacts are written.
    """

    if statement_timeout_ms < 0:
        die(f"statement timeout must be >= 0 (got {statement_timeout_ms})")

    # Stage 1: create or locate the output directory and verify that the target
    # PostgreSQL server can run the selected scenario/variant GUCs.
    run_id = (
        resume_run_id
        if resume_run_id
        else f"{utc_now().strftime('%Y%m%d_%H%M%S_%f')}_{safe_artifact_name(scenario.name)}"
    )
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_dir = OUTPUTS_DIR / run_id
    if resume_run_id:
        if not out_dir.is_dir():
            die(f"resume run_id not found: {out_dir}")
    else:
        out_dir.mkdir(parents=True, exist_ok=True)

    dbs = sorted({entry.db for entry in resolved_runs})
    ensure_databases_reachable(dbs, conn)
    validate_required_gucs(dbs[0], conn, scenario, variants_registry, variant_names)
    effective_variant_contexts = [
        {
            "name": variants_registry[name].name,
            "label": variants_registry[name].label,
            "session_gucs": [
                {k: v}
                for k, v in resolved_variant_session_gucs(dbs[0], conn, variants_registry[name])
            ],
        }
        for name in variant_names
    ]

    # Stage 2: initialize the run state.  Resume state is loaded as one unit so
    # artifact-rebuild details do not interrupt the main execution path.
    print(f"[run] scenario={scenario.name}")
    print(f"[run] variants={','.join(variant_names)}")
    print(f"[run] warmup_passes={WARMUP_RUNS} measured_reps={MEASURED_REPS}")
    print(f"[run] outputs={out_dir}")

    state = RunState()
    query_counts: list[dict[str, Any]] = []
    stabilized_dbs: set[str] = set()
    prepared_runs: list[dict[str, Any]] = []

    # Stage 3: stabilize fresh databases, then resolve all query work before
    # execution.  This makes run.json describe the intended run shape even if
    # the process is interrupted later.
    for spec in resolved_runs:
        # A fresh run owns one statistics snapshot per prepared database.  Resume
        # keeps the existing snapshot so one artifact does not mix pre/post-ANALYZE
        # groups.
        if resume_run_id is None and spec.db not in stabilized_dbs:
            stabilize_db(spec.db, conn)
            stabilized_dbs.add(spec.db)

        # Resolve query SQL before executing anything so the run context records
        # the complete dataset/query/variant plan even for interrupted runs.
        queries = select_queries(spec)
        query_plans = [(q, build_statement(spec.dataset, load_sql_for_query(q))) for q in queries]
        query_counts.append(
            {
                "dataset": spec.dataset,
                "db": spec.db,
                "max_join": spec.max_join,
                "queries_selected": len(query_plans),
                "variants": list(spec.variants),
            }
        )
        print(
            f"[run] dataset={spec.dataset} db={spec.db} queries={len(query_plans)} "
            f"variants={','.join(spec.variants)} max_join={spec.max_join}"
        )

        entry_variants = [variants_registry[name] for name in spec.variants]
        prepared_runs.append(
            {
                "spec": spec,
                "entry_variants": entry_variants,
                "query_plans": query_plans,
            }
        )

    run_groups = build_run_groups(prepared_runs)

    # Stage 4: for resume, trust the artifact state and skip database
    # stabilization so one run_id does not combine two statistics snapshots.
    if resume_run_id:
        state = load_resume_state(
            out_dir=out_dir,
            run_id=run_id,
            scenario=scenario,
            tag=tag,
            statement_timeout_ms=statement_timeout_ms,
            variant_names=variant_names,
            resolved_runs=resolved_runs,
            run_groups=run_groups,
        )
        if state.completed:
            print(f"[run] resume target already completed: {run_id}")
            return

    # Persist the full run shape before executing groups.  Even interrupted runs
    # then have enough context for resume validation and post-run inspection.
    flush_outputs(
        out_dir=out_dir,
        run_id=run_id,
        scenario=scenario,
        resolved_runs=resolved_runs,
        state=state,
        tag=tag,
        statement_timeout_ms=statement_timeout_ms,
        effective_variant_contexts=effective_variant_contexts,
        query_counts=query_counts,
        total_groups=len(run_groups),
        completed=False,
    )

    # Stage 5: execute at query-group boundaries.  All variants for one warmup
    # pass or measured repetition complete before the progress marker advances.
    for group_idx, group in enumerate(run_groups):
        if group_idx < state.completed_groups:
            continue

        if group.phase == "warmup":
            state.termination = execute_warmup_group(
                scenario=scenario,
                spec=group.spec,
                query=group.query,
                stmt=group.stmt,
                query_idx=group.query_idx,
                warmup_pass=group.pass_index,
                entry_variants=group.entry_variants,
                conn=conn,
                statement_timeout_ms=statement_timeout_ms,
                warmup_failures=state.warmup_failures,
                warmup_timeout_keys=state.warmup_timeout_keys,
            )
            if state.termination is None:
                # Warmup rows are intentionally not measured artifacts; only the
                # completed group count and any timeout/error state are persisted.
                state.completed_groups = group_idx + 1
        else:
            state.termination = execute_measured_group(
                scenario=scenario,
                spec=group.spec,
                query=group.query,
                stmt=group.stmt,
                query_idx=group.query_idx,
                rep=group.pass_index,
                entry_variants=group.entry_variants,
                conn=conn,
                statement_timeout_ms=statement_timeout_ms,
                warmup_timeout_keys=state.warmup_timeout_keys,
                raw_rows=state.raw_rows,
                summary_acc=state.summary_acc,
            )
            if state.termination is None:
                state.completed_groups = group_idx + 1

        flush_outputs(
            out_dir=out_dir,
            run_id=run_id,
            scenario=scenario,
            resolved_runs=resolved_runs,
            state=state,
            tag=tag,
            statement_timeout_ms=statement_timeout_ms,
            effective_variant_contexts=effective_variant_contexts,
            query_counts=query_counts,
            total_groups=len(run_groups),
            completed=False,
        )
        if state.termination is not None:
            break
    # Stage 6: mark the final progress state and summarize failures.
    flush_outputs(
        out_dir=out_dir,
        run_id=run_id,
        scenario=scenario,
        resolved_runs=resolved_runs,
        state=state,
        tag=tag,
        statement_timeout_ms=statement_timeout_ms,
        effective_variant_contexts=effective_variant_contexts,
        query_counts=query_counts,
        total_groups=len(run_groups),
        completed=state.termination is None,
    )

    warmup_timeout_rows = [
        row for row in state.warmup_failures if row["category"] == "statement_timeout"
    ]
    warmup_error_rows = [row for row in state.warmup_failures if row["category"] == "error"]
    skipped_timeout_rows = [
        row
        for row in state.raw_rows
        if row["status"] == "timeout"
        and row["error"].startswith("skipped measured run after warmup timeout:")
    ]
    timeout_rows = [
        row
        for row in state.raw_rows
        if row["status"] == "timeout"
        and not row["error"].startswith("skipped measured run after warmup timeout:")
    ]
    err_rows = [row for row in state.raw_rows if row["status"] == "error"]

    print_failure_rows(label="warmup_timeouts", rows=warmup_timeout_rows)
    print_failure_rows(label="warmup_errors", rows=warmup_error_rows)
    print_failure_rows(label="skipped_timeouts", rows=skipped_timeout_rows)
    print_failure_rows(label="timeouts", rows=timeout_rows)

    if state.termination is not None:
        print(
            f"[run] terminated phase={state.termination['phase']} "
            f"dataset={state.termination['dataset']} "
            f"variant={state.termination['variant']} query={state.termination['query_id']}: "
            f"{state.termination['error']}"
        )
        raise SystemExit(1)

    if err_rows:
        print_failure_rows(label="errors", rows=err_rows)
    elif state.warmup_failures or skipped_timeout_rows or timeout_rows:
        print("[run] completed with non-fatal failures")
    else:
        print("[run] completed without errors")


@dataclass
class RunState:
    """Mutable artifact/progress state for a benchmark run.

    Fresh runs start with empty state.  Resume rebuilds the same fields from
    durable artifacts, so the execution loop can treat fresh and resumed runs
    uniformly.
    """

    raw_rows: list[dict[str, str]] = field(default_factory=list)
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]] = field(default_factory=dict)
    warmup_failures: list[dict[str, Any]] = field(default_factory=list)
    warmup_timeout_keys: set[tuple[str, str, str]] = field(default_factory=set)
    completed_groups: int = 0
    termination: dict[str, Any] | None = None
    completed: bool = False


@dataclass(frozen=True)
class RunGroup:
    """One checkpointable warmup or measured group in run order."""

    phase: str
    spec: ResolvedDatasetRun
    query: QueryMeta
    stmt: str
    query_idx: int
    pass_index: int
    entry_variants: list[Variant]


def load_resume_state(
    *,
    out_dir: Path,
    run_id: str,
    scenario: Scenario,
    tag: str,
    statement_timeout_ms: int,
    variant_names: tuple[str, ...],
    resolved_runs: list[ResolvedDatasetRun],
    run_groups: list[RunGroup],
) -> RunState:
    """Validate a resume target and rebuild run state from its artifacts."""

    run_context_path = out_dir / "run.json"
    if not run_context_path.is_file():
        die(f"resume run_id is missing run.json: {run_context_path}")

    run_context = json.loads(run_context_path.read_text())
    if run_context.get("termination") is not None:
        die("resume run_id has a fatal termination record; start a new run instead")

    validate_resume_context(
        run_context,
        scenario=scenario,
        tag=tag,
        statement_timeout_ms=statement_timeout_ms,
        variant_names=variant_names,
        resolved_runs=resolved_runs,
    )

    progress = run_context.get("progress", {})
    completed_groups = load_completed_group_count(progress, run_groups)
    state = RunState(
        completed=progress.get("completed") is True,
        completed_groups=completed_groups,
    )
    if state.completed:
        return state

    state.raw_rows = load_raw_rows(out_dir / "raw.csv")
    state.summary_acc = build_summary_acc_from_raw_rows(state.raw_rows)
    state.warmup_failures = list(run_context.get("warmup_failures", []))
    state.warmup_timeout_keys = {
        (str(row["dataset"]), str(row["query_id"]), str(row["variant"]))
        for row in state.warmup_failures
        if row.get("category") == "statement_timeout"
    }
    print(
        f"[run] resume_run_id={run_id} "
        f"completed_groups={state.completed_groups}/{len(run_groups)}"
    )
    return state


def execute_warmup_group(
    *,
    scenario: Scenario,
    spec: ResolvedDatasetRun,
    query: QueryMeta,
    stmt: str,
    query_idx: int,
    warmup_pass: int,
    entry_variants: list[Variant],
    conn: Optional[ConnOpts],
    statement_timeout_ms: int,
    warmup_failures: list[dict[str, Any]],
    warmup_timeout_keys: set[tuple[str, str, str]],
) -> Optional[dict[str, Any]]:
    """Run one discarded warmup group and return a fatal termination if needed.

    A warmup group is one query plus all selected variants for one warmup pass.
    Warmup timing rows are not written to raw.csv, but timeout/error state is
    recorded so the measured phase can skip variants that already exhausted the
    statement timeout during warmup.
    """

    ordered_variants = rotate_variants(entry_variants, query_idx + warmup_pass - 1)
    for variant in ordered_variants:
        try:
            run_one(
                spec.db,
                scenario.session_gucs,
                variant,
                stmt,
                conn=conn,
                statement_timeout_ms=statement_timeout_ms,
            )
        except StatementTimeoutError as e:
            warmup_timeout_keys.add((spec.dataset, query.query_id, variant.name))
            record_warmup_failure(
                warmup_failures=warmup_failures,
                warmup_pass=warmup_pass,
                spec=spec,
                variant=variant,
                query=query,
                error=str(e),
                category="statement_timeout",
            )
        except Exception as e:
            record_warmup_failure(
                warmup_failures=warmup_failures,
                warmup_pass=warmup_pass,
                spec=spec,
                variant=variant,
                query=query,
                error=str(e),
                category="error",
            )
            print("[run] non-timeout warmup error; measured runs will be skipped")
            return {
                "phase": "warmup",
                "category": "error",
                "dataset": spec.dataset,
                "db": spec.db,
                "variant": variant.name,
                "query_id": query.query_id,
                "error": str(e),
            }
    return None


def execute_measured_group(
    *,
    scenario: Scenario,
    spec: ResolvedDatasetRun,
    query: QueryMeta,
    stmt: str,
    query_idx: int,
    rep: int,
    entry_variants: list[Variant],
    conn: Optional[ConnOpts],
    statement_timeout_ms: int,
    warmup_timeout_keys: set[tuple[str, str, str]],
    raw_rows: list[dict[str, str]],
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]],
) -> Optional[dict[str, Any]]:
    """Run one measured query group and append raw/summary accumulator rows.

    A measured group is one query plus all selected variants for one measured
    repetition.  The caller checkpoints only after the group completes, so
    resume never has to reason about partially written variant rows.  A fatal
    non-timeout error stops the group immediately after the error row is
    recorded; such runs are not resumable.
    """

    ordered_variants = rotate_variants(entry_variants, query_idx + rep - 1)
    termination: dict[str, Any] | None = None
    for variant in ordered_variants:
        key = (spec.dataset, query.query_id, variant.name)
        status = "ok"
        err = ""
        planning_ms = -1.0
        total_ms = -1.0
        exec_ms = -1.0
        plan_total_cost = -1.0

        if key in warmup_timeout_keys:
            status = "timeout"
            err = warmup_timeout_skip_error("ERROR: canceling statement due to statement timeout")
        else:
            try:
                metrics = run_one(
                    spec.db,
                    scenario.session_gucs,
                    variant,
                    stmt,
                    conn=conn,
                    statement_timeout_ms=statement_timeout_ms,
                )
                planning_ms = metrics.planning_ms
                total_ms = metrics.total_ms
                plan_total_cost = metrics.plan_total_cost
                exec_ms = metrics.execution_ms
            except StatementTimeoutError as e:
                status = "timeout"
                err = str(e)
            except Exception as e:
                status = "error"
                err = str(e)
                if termination is None:
                    termination = {
                        "phase": "measured",
                        "category": "error",
                        "dataset": spec.dataset,
                        "db": spec.db,
                        "variant": variant.name,
                        "query_id": query.query_id,
                        "rep": rep,
                        "error": str(e),
                    }

        raw_rows.append(
            {
                "dataset": spec.dataset,
                "query_id": query.query_id,
                "variant": variant.name,
                "rep": str(rep),
                "planning_ms": f"{planning_ms:.3f}" if planning_ms >= 0 else "",
                "execution_ms": f"{exec_ms:.3f}" if exec_ms >= 0 else "",
                "total_ms": f"{total_ms:.3f}" if total_ms >= 0 else "",
                "plan_total_cost": f"{plan_total_cost:.3f}" if plan_total_cost >= 0 else "",
                "status": status,
                "error": err,
            }
        )

        summary_acc.setdefault(key, []).append(
            {
                "rep": rep,
                "planning_ms": planning_ms,
                "total_ms": total_ms,
                "execution_ms": exec_ms,
                "plan_total_cost": plan_total_cost,
                "status": status,
            }
        )
        if termination is not None:
            break
    return termination


def record_warmup_failure(
    *,
    warmup_failures: list[dict[str, Any]],
    warmup_pass: int,
    spec: ResolvedDatasetRun,
    variant: Variant,
    query: QueryMeta,
    error: str,
    category: str,
) -> None:
    """Record a warmup timeout/error in run.json state and console output."""

    warmup_failures.append(
        {
            "warmup_pass": warmup_pass,
            "dataset": spec.dataset,
            "db": spec.db,
            "variant": variant.name,
            "query_id": query.query_id,
            "category": category,
            "error": error,
        }
    )
    label = "warmup_timeout" if category == "statement_timeout" else "warmup_error"
    print(
        f"[run] {label} dataset={spec.dataset} variant={variant.name} "
        f"query={query.query_id}: {error}"
    )


def warmup_timeout_skip_error(original_error: str) -> str:
    """Return the measured-row error used when warmup already timed out."""

    return f"skipped measured run after warmup timeout: {original_error}"


def print_failure_rows(*, label: str, rows: list[dict[str, str]]) -> None:
    """Print a compact failure sample without flooding long benchmark logs."""

    if not rows:
        return
    print(f"[run] {label}={len(rows)}")
    for row in rows[:5]:
        print(
            f"[run] {label[:-1]} dataset={row['dataset']} variant={row['variant']} "
            f"query={row['query_id']}: {row['error']}"
        )
    if len(rows) > 5:
        print(f"[run] ... and {len(rows) - 5} more {label}")


def rotate_variants(variants: list[Variant], offset: int) -> list[Variant]:
    """Rotate variant execution order for one query group."""

    if not variants:
        return []
    normalized = offset % len(variants)
    if normalized == 0:
        return list(variants)
    return list(variants[normalized:]) + list(variants[:normalized])


def build_run_groups(prepared_runs: list[dict[str, Any]]) -> list[RunGroup]:
    """Return the linear checkpoint sequence for warmup and measured work."""

    groups: list[RunGroup] = []
    for prepared in prepared_runs:
        spec = prepared["spec"]
        entry_variants = prepared["entry_variants"]
        query_plans = prepared["query_plans"]
        for query_idx, (query, stmt) in enumerate(query_plans):
            for warmup_pass in range(1, WARMUP_RUNS + 1):
                groups.append(
                    RunGroup(
                        phase="warmup",
                        spec=spec,
                        query=query,
                        stmt=stmt,
                        query_idx=query_idx,
                        pass_index=warmup_pass,
                        entry_variants=entry_variants,
                    )
                )
            for rep in range(1, MEASURED_REPS + 1):
                groups.append(
                    RunGroup(
                        phase="measured",
                        spec=spec,
                        query=query,
                        stmt=stmt,
                        query_idx=query_idx,
                        pass_index=rep,
                        entry_variants=entry_variants,
                    )
                )
    return groups


def dataset_contexts(resolved_runs: list[ResolvedDatasetRun]) -> list[dict[str, Any]]:
    """Return the resume-validation view of selected datasets."""

    return [
        {
            "dataset": spec.dataset,
            "max_join": spec.max_join,
            "variants": list(spec.variants),
        }
        for spec in resolved_runs
    ]


def load_raw_rows(raw_path: Path) -> list[dict[str, str]]:
    """Load existing raw.csv rows for resume, or return an empty run state."""

    if not raw_path.is_file():
        return []
    with raw_path.open(newline="") as f:
        return list(csv.DictReader(f))


def build_summary_acc_from_raw_rows(
    raw_rows: list[dict[str, str]],
) -> dict[tuple[str, str, str], list[dict[str, object]]]:
    """Rebuild summary accumulators from durable raw.csv rows during resume."""

    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    for row in raw_rows:
        key = (row["dataset"], row["query_id"], row["variant"])
        summary_acc.setdefault(key, []).append(
            {
                "rep": int(row["rep"]),
                "planning_ms": float(row["planning_ms"]) if row["planning_ms"] else -1.0,
                "total_ms": float(row["total_ms"]) if row["total_ms"] else -1.0,
                "execution_ms": float(row["execution_ms"]) if row["execution_ms"] else -1.0,
                "plan_total_cost": (
                    float(row["plan_total_cost"]) if row["plan_total_cost"] else -1.0
                ),
                "status": row["status"],
            }
        )
    return summary_acc


def load_completed_group_count(
    progress: dict[str, Any],
    run_groups: list[RunGroup],
) -> int:
    """Return the completed linear group count stored in run.json progress."""

    try:
        completed_groups = int(progress["completed_groups"])
        total_groups = int(progress["total_groups"])
    except KeyError as e:
        die(f"resume run_id is missing progress field: {e.args[0]}")

    if total_groups != len(run_groups):
        die(
            "resume run_id total_groups does not match requested work: "
            f"existing={total_groups!r}, expected={len(run_groups)}"
        )
    if completed_groups < 0 or completed_groups > len(run_groups):
        die(
            "resume run_id has invalid completed_groups: "
            f"{completed_groups} for total_groups={len(run_groups)}"
        )
    return completed_groups


def validate_resume_context(
    run_context: dict[str, Any],
    *,
    scenario: Scenario,
    tag: str,
    statement_timeout_ms: int,
    variant_names: tuple[str, ...],
    resolved_runs: list[ResolvedDatasetRun],
) -> None:
    """Reject resume attempts that would change the original run protocol."""

    if run_context.get("scenario") != scenario.name:
        die(
            f"resume run_id belongs to scenario '{run_context.get('scenario')}', "
            f"expected '{scenario.name}'"
        )

    if (run_context.get("tag") or "") != tag:
        die(f"resume run_id has tag '{run_context.get('tag', '')}', expected '{tag}'")

    existing_timeout = run_context.get("statement_timeout_ms")
    if existing_timeout != statement_timeout_ms:
        die(
            "resume run_id statement_timeout_ms mismatch: "
            f"existing={existing_timeout!r}, expected={statement_timeout_ms!r}"
        )

    existing_variants = tuple(str(item.get("name")) for item in run_context.get("variants", []))
    if existing_variants != variant_names:
        die(
            f"resume run_id variant mismatch: existing={','.join(existing_variants)}, "
            f"expected={','.join(variant_names)}"
        )

    existing_datasets = run_context.get("datasets", [])
    expected_datasets = dataset_contexts(resolved_runs)
    if existing_datasets != expected_datasets:
        die("resume run_id dataset selection or ordering does not match the requested run")


def flush_outputs(
    *,
    out_dir: Path,
    run_id: str,
    scenario: Scenario,
    resolved_runs: list[ResolvedDatasetRun],
    state: RunState,
    tag: str,
    statement_timeout_ms: int,
    effective_variant_contexts: list[dict[str, Any]],
    query_counts: list[dict[str, Any]],
    total_groups: int,
    completed: bool,
) -> None:
    """Write all resumable artifacts from the current in-memory run state."""

    raw_path = out_dir / "raw.csv"
    write_raw_csv(raw_path, state.raw_rows)

    summary_path = out_dir / "summary.csv"
    write_summary_csv(
        summary_path,
        resolved_runs=resolved_runs,
        summary_acc=state.summary_acc,
    )

    run_context = build_run_context(
        run_id=run_id,
        scenario=scenario,
        tag=tag,
        statement_timeout_ms=statement_timeout_ms,
        effective_variant_contexts=effective_variant_contexts,
        query_counts=query_counts,
    )
    if state.warmup_failures:
        run_context["warmup_failures"] = state.warmup_failures
    if state.termination is not None:
        run_context["termination"] = state.termination
    run_context["progress"] = {
        "completed": completed,
        "completed_groups": state.completed_groups,
        "total_groups": total_groups,
    }

    write_run_context(out_dir / "run.json", run_context)
