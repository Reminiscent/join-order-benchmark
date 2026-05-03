"""Execution driver for ``bench.py run``.

The file starts with ``run_scenario()`` so readers see the benchmark workflow
before the lower-level checkpointing, resume, and reporting helpers.
"""

from __future__ import annotations

import csv
import json
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
    fail_on_error: bool,
) -> None:
    """Execute one resolved benchmark scenario and maintain resumable artifacts.

    A run is checkpointed only after whole query groups complete.  That keeps
    ``raw.csv``, ``summary.csv``, and ``run.json`` consistent enough for
    ``--resume-run-id`` to rebuild in-memory progress without replaying partial
    groups.  Fresh runs stabilize each prepared database before any query is
    executed; resumed runs reuse the existing statistics snapshot recorded by
    the partial artifact.
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

    # Stage 2: initialize the mutable run state.  These structures mirror the
    # durable artifacts and are rebuilt from disk when resuming.
    print(f"[run] scenario={scenario.name}")
    print(f"[run] variants={','.join(variant_names)}")
    print(f"[run] warmup_passes={WARMUP_RUNS} measured_reps={MEASURED_REPS}")
    print(f"[run] outputs={out_dir}")

    raw_rows: list[dict[str, str]] = []
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    query_counts: list[dict[str, Any]] = []
    stabilized_dbs: set[str] = set()
    prepared_runs: list[dict[str, Any]] = []
    warmup_failures: list[dict[str, Any]] = []
    warmup_timeout_keys: set[tuple[str, str, str]] = set()
    completed_warmup_groups: set[tuple[int, str, str]] = set()
    completed_measured_groups: set[tuple[str, str, int]] = set()
    termination: dict[str, Any] | None = None

    # Stage 3: resolve all work before execution.  This makes run.json describe
    # the intended run shape even if the process is interrupted later.
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

    # Stage 4: for resume, trust the artifact state and skip database
    # stabilization so one run_id does not combine two statistics snapshots.
    if resume_run_id:
        run_context_path = out_dir / "run.json"
        if not run_context_path.is_file():
            die(f"resume run_id is missing run.json: {run_context_path}")
        run_context = json.loads(run_context_path.read_text())
        validate_resume_context(
            run_context,
            scenario=scenario,
            tag=tag,
            statement_timeout_ms=statement_timeout_ms,
            variant_names=variant_names,
            resolved_runs=resolved_runs,
        )
        if run_context.get("progress", {}).get("completed") is True:
            print(f"[run] resume target already completed: {run_id}")
            return
        # Rebuild in-memory state from durable artifacts.  Progress is tracked at
        # whole warmup-group and whole measured-group granularity only.
        raw_rows = load_raw_rows(out_dir / "raw.csv")
        summary_acc = build_summary_acc_from_raw_rows(raw_rows)
        warmup_failures = list(run_context.get("warmup_failures", []))
        warmup_timeout_keys = {
            (str(row["dataset"]), str(row["query_id"]), str(row["variant"]))
            for row in warmup_failures
            if row.get("category") == "statement_timeout"
        }
        progress = run_context.get("progress", {})
        completed_warmup_groups = deserialize_warmup_groups(
            progress.get("completed_warmup_groups", [])
        )
        completed_measured_groups = deserialize_measured_groups(
            progress.get("completed_measured_groups", [])
        )
        print(
            f"[run] resume_run_id={run_id} completed_warmup_groups={len(completed_warmup_groups)} "
            f"completed_measured_groups={len(completed_measured_groups)}"
        )

    # Persist the full run shape before executing groups.  Even interrupted runs
    # then have enough context for resume validation and post-run inspection.
    flush_outputs(
        out_dir=out_dir,
        run_id=run_id,
        scenario=scenario,
        resolved_runs=resolved_runs,
        raw_rows=raw_rows,
        summary_acc=summary_acc,
        tag=tag,
        statement_timeout_ms=statement_timeout_ms,
        effective_variant_contexts=effective_variant_contexts,
        query_counts=query_counts,
        warmup_failures=warmup_failures,
        termination=termination,
        completed_warmup_groups=completed_warmup_groups,
        completed_measured_groups=completed_measured_groups,
        completed=False,
    )

    # Stage 5: execute at query-group boundaries.  All variants for one warmup
    # pass or measured repetition complete before the progress marker advances.
    for prepared in prepared_runs:
        if termination is not None:
            break
        spec = prepared["spec"]
        entry_variants = prepared["entry_variants"]
        query_plans = prepared["query_plans"]

        for query_idx, (q, stmt) in enumerate(query_plans):
            if termination is not None:
                break

            for warmup_pass in range(1, WARMUP_RUNS + 1):
                warmup_group = (warmup_pass, spec.dataset, q.query_id)
                if warmup_group in completed_warmup_groups:
                    continue

                termination = execute_warmup_group(
                    scenario=scenario,
                    spec=spec,
                    query=q,
                    stmt=stmt,
                    query_idx=query_idx,
                    warmup_pass=warmup_pass,
                    entry_variants=entry_variants,
                    conn=conn,
                    statement_timeout_ms=statement_timeout_ms,
                    fail_on_error=fail_on_error,
                    warmup_failures=warmup_failures,
                    warmup_timeout_keys=warmup_timeout_keys,
                )
                if termination is not None:
                    break
                # Warmup rows are intentionally not measured artifacts; only the
                # completed group marker and any timeout/error state are persisted.
                completed_warmup_groups.add(warmup_group)
                flush_outputs(
                    out_dir=out_dir,
                    run_id=run_id,
                    scenario=scenario,
                    resolved_runs=resolved_runs,
                    raw_rows=raw_rows,
                    summary_acc=summary_acc,
                    tag=tag,
                    statement_timeout_ms=statement_timeout_ms,
                    effective_variant_contexts=effective_variant_contexts,
                    query_counts=query_counts,
                    warmup_failures=warmup_failures,
                    termination=termination,
                    completed_warmup_groups=completed_warmup_groups,
                    completed_measured_groups=completed_measured_groups,
                    completed=False,
                )

            if termination is not None:
                break

            for rep in range(1, MEASURED_REPS + 1):
                measured_group = (spec.dataset, q.query_id, rep)
                if measured_group in completed_measured_groups:
                    continue
                execute_measured_group(
                    scenario=scenario,
                    spec=spec,
                    query=q,
                    stmt=stmt,
                    query_idx=query_idx,
                    rep=rep,
                    entry_variants=entry_variants,
                    conn=conn,
                    statement_timeout_ms=statement_timeout_ms,
                    warmup_timeout_keys=warmup_timeout_keys,
                    raw_rows=raw_rows,
                    summary_acc=summary_acc,
                )
                completed_measured_groups.add(measured_group)
                flush_outputs(
                    out_dir=out_dir,
                    run_id=run_id,
                    scenario=scenario,
                    resolved_runs=resolved_runs,
                    raw_rows=raw_rows,
                    summary_acc=summary_acc,
                    tag=tag,
                    statement_timeout_ms=statement_timeout_ms,
                    effective_variant_contexts=effective_variant_contexts,
                    query_counts=query_counts,
                    warmup_failures=warmup_failures,
                    termination=termination,
                    completed_warmup_groups=completed_warmup_groups,
                    completed_measured_groups=completed_measured_groups,
                    completed=False,
                )
    # Stage 6: mark the final progress state and summarize non-fatal failures.
    flush_outputs(
        out_dir=out_dir,
        run_id=run_id,
        scenario=scenario,
        resolved_runs=resolved_runs,
        raw_rows=raw_rows,
        summary_acc=summary_acc,
        tag=tag,
        statement_timeout_ms=statement_timeout_ms,
        effective_variant_contexts=effective_variant_contexts,
        query_counts=query_counts,
        warmup_failures=warmup_failures,
        termination=termination,
        completed_warmup_groups=completed_warmup_groups,
        completed_measured_groups=completed_measured_groups,
        completed=termination is None,
    )

    warmup_timeout_rows = [row for row in warmup_failures if row["category"] == "statement_timeout"]
    warmup_error_rows = [row for row in warmup_failures if row["category"] == "error"]
    skipped_timeout_rows = [
        row
        for row in raw_rows
        if row["status"] == "timeout"
        and row["error"].startswith("skipped measured run after warmup timeout:")
    ]
    timeout_rows = [
        row
        for row in raw_rows
        if row["status"] == "timeout"
        and not row["error"].startswith("skipped measured run after warmup timeout:")
    ]
    err_rows = [row for row in raw_rows if row["status"] == "error"]

    print_failure_rows(label="warmup_timeouts", rows=warmup_timeout_rows)
    print_failure_rows(label="warmup_errors", rows=warmup_error_rows)
    print_failure_rows(label="skipped_timeouts", rows=skipped_timeout_rows)
    print_failure_rows(label="timeouts", rows=timeout_rows)

    if termination is not None:
        print(
            f"[run] terminated phase={termination['phase']} dataset={termination['dataset']} "
            f"variant={termination['variant']} query={termination['query_id']}: "
            f"{termination['error']}"
        )
        raise SystemExit(1)

    if err_rows:
        print_failure_rows(label="errors", rows=err_rows)
        if fail_on_error:
            raise SystemExit(1)
    elif warmup_failures or skipped_timeout_rows or timeout_rows:
        print("[run] completed with non-fatal failures")
    else:
        print("[run] completed without errors")


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
    fail_on_error: bool,
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
            if fail_on_error:
                print("[run] fail_on_error triggered during warmup; measured runs will be skipped")
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
) -> None:
    """Run one measured query group and append raw/summary accumulator rows.

    A measured group is one query plus all selected variants for one measured
    repetition.  The caller checkpoints only after the group completes, so
    resume never has to reason about partially written variant rows.
    """

    ordered_variants = rotate_variants(entry_variants, query_idx + rep - 1)
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


def serialize_warmup_groups(groups: set[tuple[int, str, str]]) -> list[dict[str, object]]:
    """Convert completed warmup groups into run.json progress records."""

    return [
        {"warmup_pass": warmup_pass, "dataset": dataset, "query_id": query_id}
        for warmup_pass, dataset, query_id in sorted(groups)
    ]


def deserialize_warmup_groups(items: list[dict[str, object]]) -> set[tuple[int, str, str]]:
    """Load completed warmup groups from run.json progress records."""

    return {
        (int(item["warmup_pass"]), str(item["dataset"]), str(item["query_id"]))
        for item in items
    }


def serialize_measured_groups(groups: set[tuple[str, str, int]]) -> list[dict[str, object]]:
    """Convert completed measured groups into run.json progress records."""

    return [
        {"dataset": dataset, "query_id": query_id, "rep": rep}
        for dataset, query_id, rep in sorted(groups)
    ]


def deserialize_measured_groups(items: list[dict[str, object]]) -> set[tuple[str, str, int]]:
    """Load completed measured groups from run.json progress records."""

    return {
        (str(item["dataset"]), str(item["query_id"]), int(item["rep"]))
        for item in items
    }


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
    raw_rows: list[dict[str, str]],
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]],
    tag: str,
    statement_timeout_ms: int,
    effective_variant_contexts: list[dict[str, Any]],
    query_counts: list[dict[str, Any]],
    warmup_failures: list[dict[str, Any]],
    termination: Optional[dict[str, Any]],
    completed_warmup_groups: set[tuple[int, str, str]],
    completed_measured_groups: set[tuple[str, str, int]],
    completed: bool,
) -> None:
    """Write all resumable artifacts from the current in-memory run state."""

    raw_path = out_dir / "raw.csv"
    write_raw_csv(raw_path, raw_rows)

    summary_path = out_dir / "summary.csv"
    write_summary_csv(
        summary_path,
        resolved_runs=resolved_runs,
        summary_acc=summary_acc,
    )

    run_context = build_run_context(
        run_id=run_id,
        scenario=scenario,
        tag=tag,
        statement_timeout_ms=statement_timeout_ms,
        effective_variant_contexts=effective_variant_contexts,
        query_counts=query_counts,
    )
    if warmup_failures:
        run_context["warmup_failures"] = warmup_failures
    if termination is not None:
        run_context["termination"] = termination
    run_context["progress"] = {
        "completed": completed,
        "completed_warmup_groups": serialize_warmup_groups(completed_warmup_groups),
        "completed_measured_groups": serialize_measured_groups(completed_measured_groups),
    }

    write_run_context(out_dir / "run.json", run_context)
