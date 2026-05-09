"""Scenario runner for benchmark execution.

This module coordinates statistics setup, query selection, warmup/measured
execution, timeout classification, and artifact flushing for one run.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from bench_config import build_statement, load_sql_for_query, select_queries
from bench_common import (
    ConnOpts,
    OUTPUTS_DIR,
    QueryMeta,
    ResolvedDatasetRun,
    Scenario,
    Variant,
    safe_artifact_name,
    utc_now,
)
from bench_exec import (
    StatementTimeoutError,
    dump_statistics,
    ensure_databases_reachable,
    run_one_statement,
    stabilize_db,
    validate_session_gucs,
)
from bench_results import build_run_context, write_raw_csv, write_run_context, write_summary_csv


MEASURED_REPS = 3
WARMUP_RUNS = 1
# A warmup timeout is benchmark data, not a harness failure.  Keep its marker
# stable because measured rows use it to distinguish skipped timeout rows.
WARMUP_TIMEOUT_CATEGORY = "statement_timeout"
WARMUP_TIMEOUT_SKIP_PREFIX = "skipped measured run after warmup timeout:"


# Run data models.


@dataclass
class RunState:
    """Mutable artifact rows and stop state for one benchmark run."""

    raw_rows: list[dict[str, str]] = field(default_factory=list)
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]] = field(default_factory=dict)
    # Warmup failures are not measured rows, but run.json records them.
    warmup_failures: list[dict[str, Any]] = field(default_factory=list)
    warmup_timeout_keys: set[tuple[str, str, str]] = field(default_factory=set)
    termination: dict[str, Any] | None = None
    elapsed_seconds: float | None = None


@dataclass(frozen=True)
class PreparedRunWork:
    """One dataset's ready-to-run work bundle.

    For example, running JOB with variants dp/geqo and queries 1a/2a creates
    one PreparedRunWork:
    - spec: JOB dataset/db/min_join plus the variant names ("dp", "geqo")
    - selected_variants: the Variant objects for dp and geqo
    - query_statements: query 1a SQL and query 2a SQL, ready for EXPLAIN
    """

    # Dataset, database, min_join, and variant-name choices for this run entry.
    spec: ResolvedDatasetRun
    # Variant objects for spec.variants, preserving the requested order.
    selected_variants: list[Variant]
    # Selected queries paired with the SQL statement sent to PostgreSQL.
    query_statements: list[tuple[QueryMeta, str]]


# Scenario orchestration.


def run_scenario(
    scenario: Scenario,
    variants_registry: dict[str, Variant],
    variant_names: tuple[str, ...],
    resolved_runs: list[ResolvedDatasetRun],
    *,
    conn: Optional[ConnOpts],
    run_session_gucs: tuple[tuple[str, Any], ...],
    tag: str,
    reuse_stats: bool = False,
) -> None:
    """Execute one resolved benchmark scenario and write run artifacts.

    Each invocation creates a new output directory.  By default, the runner
    refreshes or reuses database statistics before executing queries, writes
    local artifacts as work completes, records ``statement_timeout`` as
    benchmark data, and exits non-zero on non-timeout query-execution errors
    after writing the current artifacts.
    """

    started_at = time.perf_counter()
    stats_refresh = "reuse_existing" if reuse_stats else "before_run"

    # Stage 1: create the output directory and verify that the target PostgreSQL
    # server can run the shared and selected variant GUCs.
    run_id = f"{utc_now().strftime('%Y%m%d_%H%M%S_%f')}_{safe_artifact_name(scenario.name)}"
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_dir = OUTPUTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    dbs = sorted({entry.db for entry in resolved_runs})
    ensure_databases_reachable(dbs, conn)
    validate_session_gucs(dbs[0], conn, run_session_gucs, variants_registry, variant_names)
    # Snapshot the variant GUCs that will be SET for this run.
    effective_variant_contexts = [
        {
            "name": variants_registry[name].name,
            "label": variants_registry[name].label,
            "baseline": variants_registry[name].baseline,
            "session_gucs": [{k: v} for k, v in variants_registry[name].session_gucs],
        }
        for name in variant_names
    ]

    # Stage 2: initialize run state and print the run header.
    state = RunState()
    print(f"[run] scenario={scenario.name}")
    print(f"[run] variants={','.join(variant_names)}")
    print(f"[run] warmup_passes={WARMUP_RUNS} measured_reps={MEASURED_REPS}")
    print(f"[run] stats_refresh={stats_refresh}")
    print(f"[run] outputs={out_dir}")

    # Stage 3: select/load all query work, then refresh stats if needed and dump them.
    dataset_contexts, prepared_runs = prepare_dataset_work(
        resolved_runs=resolved_runs,
        variants_registry=variants_registry,
        conn=conn,
        reuse_stats=reuse_stats,
        stats_dir=out_dir / "stats",
    )

    # Stage 4: write the initial artifacts so the output directory immediately
    # records the requested scenario, datasets, and variants.
    def write_current_artifacts() -> None:
        # Flush the whole artifact set from memory so interrupted runs remain inspectable.
        flush_outputs(
            out_dir=out_dir,
            run_id=run_id,
            scenario=scenario,
            resolved_runs=resolved_runs,
            state=state,
            tag=tag,
            run_session_gucs=run_session_gucs,
            effective_variant_contexts=effective_variant_contexts,
            dataset_contexts=dataset_contexts,
            stats_refresh=stats_refresh,
        )

    write_current_artifacts()

    # Stage 5: execute warmup and measured groups, flushing artifacts after
    # every group so partial runs remain inspectable.
    execute_prepared_work(
        prepared_runs=prepared_runs,
        state=state,
        conn=conn,
        run_session_gucs=run_session_gucs,
        write_current_artifacts=write_current_artifacts,
    )

    # Stage 6: persist final run-level duration, then summarize failures.
    state.elapsed_seconds = round(time.perf_counter() - started_at, 3)
    write_current_artifacts()
    summarize_run_completion(state)


# Dataset work preparation.


def prepare_dataset_work(
    *,
    resolved_runs: list[ResolvedDatasetRun],
    variants_registry: dict[str, Variant],
    conn: Optional[ConnOpts],
    reuse_stats: bool,
    stats_dir: Path,
) -> tuple[list[dict[str, Any]], list[PreparedRunWork]]:
    """Prepare dataset work, then refresh stats if needed and dump them.

    The returned dataset contexts are written to run.json.  The PreparedRunWork
    objects are consumed by the warmup/measured execution loop.
    """

    dataset_contexts: list[dict[str, Any]] = []
    prepared_runs: list[PreparedRunWork] = []
    distinct_dbs: list[str] = []
    seen_dbs: set[str] = set()

    for spec in resolved_runs:
        if spec.db not in seen_dbs:
            distinct_dbs.append(spec.db)
            seen_dbs.add(spec.db)

        queries = select_queries(spec)
        query_statements = [
            (query, build_statement(spec.dataset, load_sql_for_query(query)))
            for query in queries
        ]
        dataset_contexts.append(
            {
                "dataset": spec.dataset,
                "min_join": spec.min_join,
                "variants": list(spec.variants),
            }
        )
        print(
            f"[run] dataset={spec.dataset} db={spec.db} queries={len(query_statements)} "
            f"variants={','.join(spec.variants)} min_join={spec.min_join}"
        )

        prepared_runs.append(
            PreparedRunWork(
                spec=spec,
                selected_variants=[variants_registry[name] for name in spec.variants],
                query_statements=query_statements,
            )
        )

    # Refresh only after all selected SQL is known to be loadable.
    if not reuse_stats:
        for db in distinct_dbs:
            stabilize_db(db, conn)

    dump_run_statistics(distinct_dbs, stats_dir, conn)

    return dataset_contexts, prepared_runs


# Statistics artifact dumping.


def dump_run_statistics(
    dbs: list[str],
    stats_dir: Path,
    conn: Optional[ConnOpts],
) -> None:
    """Dump one statistics restore script per physical benchmark database."""

    for db in dbs:
        path = stats_dir / f"{safe_artifact_name(db)}.sql"
        dump_statistics(db, path, conn)
        print(f"[run] stats_dump db={db} path={path}")


# Warmup and measured execution.


def execute_prepared_work(
    *,
    prepared_runs: list[PreparedRunWork],
    state: RunState,
    conn: Optional[ConnOpts],
    run_session_gucs: tuple[tuple[str, Any], ...],
    write_current_artifacts: Callable[[], None],
) -> None:
    """Run every prepared query group and flush artifacts after each group."""

    # Each measured repetition runs all variants once for the same query, with
    # the variant order rotated.
    for prepared in prepared_runs:
        for query_idx, (query, stmt) in enumerate(prepared.query_statements):
            for warmup_pass in range(1, WARMUP_RUNS + 1):
                state.termination = execute_warmup_group(
                    spec=prepared.spec,
                    query=query,
                    stmt=stmt,
                    query_idx=query_idx,
                    warmup_pass=warmup_pass,
                    selected_variants=prepared.selected_variants,
                    conn=conn,
                    run_session_gucs=run_session_gucs,
                    warmup_failures=state.warmup_failures,
                    warmup_timeout_keys=state.warmup_timeout_keys,
                )
                write_current_artifacts()
                if state.termination is not None:
                    return

            for rep in range(1, MEASURED_REPS + 1):
                state.termination = execute_measured_group(
                    spec=prepared.spec,
                    query=query,
                    stmt=stmt,
                    query_idx=query_idx,
                    rep=rep,
                    selected_variants=prepared.selected_variants,
                    conn=conn,
                    run_session_gucs=run_session_gucs,
                    warmup_timeout_keys=state.warmup_timeout_keys,
                    raw_rows=state.raw_rows,
                    summary_acc=state.summary_acc,
                )
                write_current_artifacts()
                if state.termination is not None:
                    return


def execute_warmup_group(
    *,
    spec: ResolvedDatasetRun,
    query: QueryMeta,
    stmt: str,
    query_idx: int,
    warmup_pass: int,
    selected_variants: list[Variant],
    conn: Optional[ConnOpts],
    run_session_gucs: tuple[tuple[str, Any], ...],
    warmup_failures: list[dict[str, Any]],
    warmup_timeout_keys: set[tuple[str, str, str]],
) -> Optional[dict[str, Any]]:
    """Run one discarded warmup group and return a fatal termination if needed.

    A warmup group is one query plus all selected variants for one warmup pass.
    Warmup timing rows are not written to raw.csv, but timeout/error state is
    recorded so the measured phase can skip variants that already exhausted the
    statement timeout during warmup.
    """

    # Example with [dp, geqo, my_algo]: offsets 0/1/2 run dp/geqo/my_algo first.
    ordered_variants = rotate_variants(selected_variants, query_idx + warmup_pass - 1)
    for variant in ordered_variants:
        try:
            run_one_statement(
                spec.db,
                run_session_gucs,
                variant,
                stmt,
                conn=conn,
            )
        except StatementTimeoutError as e:
            # Timeout is a valid benchmark outcome; measured reps will record skipped rows.
            warmup_timeout_keys.add((spec.dataset, query.query_id, variant.name))
            record_warmup_failure(
                warmup_failures=warmup_failures,
                warmup_pass=warmup_pass,
                spec=spec,
                variant=variant,
                query=query,
                error=str(e),
                category=WARMUP_TIMEOUT_CATEGORY,
            )
        except Exception as e:
            # Other warmup errors usually mean the run is invalid, so stop after recording.
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
    spec: ResolvedDatasetRun,
    query: QueryMeta,
    stmt: str,
    query_idx: int,
    rep: int,
    selected_variants: list[Variant],
    conn: Optional[ConnOpts],
    run_session_gucs: tuple[tuple[str, Any], ...],
    warmup_timeout_keys: set[tuple[str, str, str]],
    raw_rows: list[dict[str, str]],
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]],
) -> Optional[dict[str, Any]]:
    """Run one measured query group and append raw/summary accumulator rows.

    A measured group is one query plus all selected variants for one measured
    repetition.  A fatal non-timeout error stops the group immediately after the
    error row is recorded.
    """

    # Repetitions rotate too, so a fixed variant is not always first for a query.
    ordered_variants = rotate_variants(selected_variants, query_idx + rep - 1)
    termination: dict[str, Any] | None = None
    for variant in ordered_variants:
        key = (spec.dataset, query.query_id, variant.name)
        status = "ok"
        err = ""
        # Negative sentinels serialize to blank CSV cells when no metric exists.
        planning_ms = -1.0
        total_ms = -1.0
        exec_ms = -1.0
        plan_total_cost = -1.0
        explain_json = ""

        if key in warmup_timeout_keys:
            # Avoid spending measured reps on a query/variant that already timed out.
            status = "timeout"
            err = warmup_timeout_skip_error("ERROR: canceling statement due to statement timeout")
        else:
            try:
                metrics = run_one_statement(
                    spec.db,
                    run_session_gucs,
                    variant,
                    stmt,
                    conn=conn,
                )
                planning_ms = metrics.planning_ms
                total_ms = metrics.total_ms
                plan_total_cost = metrics.plan_total_cost
                exec_ms = metrics.execution_ms
                explain_json = metrics.explain_json
            except StatementTimeoutError as e:
                # A measured timeout is reported but does not abort the benchmark run.
                status = "timeout"
                err = str(e)
            except Exception as e:
                # Non-timeout execution errors are fatal, but the row is still preserved.
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

        # Keep numeric values for summary calculations; raw.csv stores formatted strings.
        summary_acc.setdefault(key, []).append(
            {
                "rep": rep,
                "planning_ms": planning_ms,
                "total_ms": total_ms,
                "execution_ms": exec_ms,
                "plan_total_cost": plan_total_cost,
                "explain_json": explain_json,
                "status": status,
            }
        )
        if termination is not None:
            break
    return termination


# Failure recording and reporting.


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
    label = "warmup_timeout" if category == WARMUP_TIMEOUT_CATEGORY else "warmup_error"
    print(
        f"[run] {label} dataset={spec.dataset} variant={variant.name} "
        f"query={query.query_id}: {error}"
    )


def warmup_timeout_skip_error(original_error: str) -> str:
    """Return the measured-row error used when warmup already timed out."""

    return f"{WARMUP_TIMEOUT_SKIP_PREFIX} {original_error}"


def print_failure_rows(*, label: str, rows: list[dict[str, Any]]) -> None:
    """Print a compact failure sample without flooding long benchmark logs."""

    if not rows:
        return
    print(f"[run] {label}={len(rows)}")
    sample_label = label.removesuffix("s")
    for row in rows[:5]:
        print(
            f"[run] {sample_label} dataset={row['dataset']} variant={row['variant']} "
            f"query={row['query_id']}: {row['error']}"
        )
    if len(rows) > 5:
        print(f"[run] ... and {len(rows) - 5} more {label}")


def summarize_run_completion(state: RunState) -> None:
    """Print the final run status and exit non-zero for fatal termination."""

    # Split timeout classes so direct measured timeouts and warmup-skipped rows
    # remain distinguishable in both logs and output artifacts.
    warmup_timeout_rows = [
        row for row in state.warmup_failures if row["category"] == WARMUP_TIMEOUT_CATEGORY
    ]
    warmup_error_rows = [row for row in state.warmup_failures if row["category"] == "error"]
    skipped_timeout_rows = [
        row
        for row in state.raw_rows
        if row["status"] == "timeout"
        and row["error"].startswith(WARMUP_TIMEOUT_SKIP_PREFIX)
    ]
    timeout_rows = [
        row
        for row in state.raw_rows
        if row["status"] == "timeout"
        and not row["error"].startswith(WARMUP_TIMEOUT_SKIP_PREFIX)
    ]
    err_rows = [row for row in state.raw_rows if row["status"] == "error"]

    print_failure_rows(label="warmup_timeouts", rows=warmup_timeout_rows)
    print_failure_rows(label="warmup_errors", rows=warmup_error_rows)
    print_failure_rows(label="skipped_timeouts", rows=skipped_timeout_rows)
    print_failure_rows(label="timeouts", rows=timeout_rows)
    if state.elapsed_seconds is not None:
        print(f"[run] elapsed_seconds={state.elapsed_seconds:.3f}")

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


# Variant ordering.


def rotate_variants(variants: list[Variant], offset: int) -> list[Variant]:
    """Rotate variant execution order for one query group."""

    if not variants:
        return []
    normalized = offset % len(variants)
    if normalized == 0:
        return list(variants)
    return list(variants[normalized:]) + list(variants[:normalized])


# Artifact flushing.


def flush_outputs(
    *,
    out_dir: Path,
    run_id: str,
    scenario: Scenario,
    resolved_runs: list[ResolvedDatasetRun],
    state: RunState,
    tag: str,
    run_session_gucs: tuple[tuple[str, Any], ...],
    effective_variant_contexts: list[dict[str, Any]],
    dataset_contexts: list[dict[str, Any]],
    stats_refresh: str,
) -> None:
    """Write all run artifacts from the current in-memory run state."""

    raw_path = out_dir / "raw.csv"
    write_raw_csv(raw_path, state.raw_rows)

    summary_path = out_dir / "summary.csv"
    write_summary_csv(
        summary_path,
        resolved_runs=resolved_runs,
        summary_acc=state.summary_acc,
        measured_reps=MEASURED_REPS,
        plans_dir=out_dir / "plans",
    )

    # run.json captures configuration/protocol metadata plus the latest failure state.
    run_context = build_run_context(
        run_id=run_id,
        scenario=scenario,
        tag=tag,
        run_session_gucs=run_session_gucs,
        measured_reps=MEASURED_REPS,
        warmup_runs=WARMUP_RUNS,
        effective_variant_contexts=effective_variant_contexts,
        dataset_contexts=dataset_contexts,
        stats_refresh=stats_refresh,
    )
    if state.warmup_failures:
        run_context["warmup_failures"] = state.warmup_failures
    if state.termination is not None:
        run_context["termination"] = state.termination
    if state.elapsed_seconds is not None:
        run_context["elapsed_seconds"] = state.elapsed_seconds

    write_run_context(out_dir / "run.json", run_context)
