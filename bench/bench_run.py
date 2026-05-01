from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Optional

from bench_catalog import build_statement, load_sql_for_query, select_queries
from bench_common import (
    ConnOpts,
    OUTPUTS_DIR,
    ResolvedDatasetRun,
    Scenario,
    Variant,
    die,
    psql_cmd,
    run_cmd,
    safe_artifact_name,
    utc_now,
)
from bench_exec import (
    StatementTimeoutError,
    current_setting,
    first_error_line,
    guc_exists,
    rotate_variants,
    run_one,
    stabilize_db,
)
from bench_results import build_run_context, write_raw_csv, write_run_context, write_summary_csv


def record_warmup_failure(
    *,
    warmup_failures: list[dict[str, Any]],
    warmup_pass: int,
    spec: ResolvedDatasetRun,
    variant: Variant,
    query: Any,
    error: str,
    category: str,
) -> None:
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
    print(f"[run] {label} dataset={spec.dataset} variant={variant.name} query={query.query_id}: {error}")


def warmup_timeout_skip_error(original_error: str) -> str:
    return f"skipped measured run after warmup timeout: {original_error}"


def print_failure_rows(*, label: str, rows: list[dict[str, str]]) -> None:
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


def ensure_databases_reachable(dbs: list[str], conn: Optional[ConnOpts] = None) -> None:
    for db in dbs:
        p = run_cmd(psql_cmd(db, conn) + ["-At"], input_text="SELECT 1;\n", check=False)
        if p.returncode == 0:
            continue
        out = (p.stdout or "") + (p.stderr or "")
        die(
            f"cannot connect to benchmark database '{db}': "
            f"{first_error_line(out) or 'connection failed'}. "
            "Run prepare first or fix the PostgreSQL connection flags."
        )


def resolved_variant_session_gucs(
    db: str,
    conn: Optional[ConnOpts],
    variant: Variant,
) -> tuple[tuple[str, object], ...]:
    resolved = list(variant.session_gucs)
    for name, value in variant.optional_session_gucs:
        if guc_exists(db, conn, name):
            resolved.append((name, value))
    return tuple(resolved)


def validate_required_gucs(
    db: str,
    conn: Optional[ConnOpts],
    scenario: Scenario,
    variants_registry: dict[str, Variant],
    variant_names: tuple[str, ...],
) -> None:
    missing_scenario = [name for name, _ in scenario.session_gucs if current_setting(db, name, conn) is None]
    if missing_scenario:
        die(
            f"scenario '{scenario.name}' requires unsupported PostgreSQL parameter(s): "
            f"{', '.join(sorted(missing_scenario))}"
        )

    missing_by_variant: list[str] = []
    for variant_name in variant_names:
        variant = variants_registry[variant_name]
        missing = [name for name, _ in variant.session_gucs if current_setting(db, name, conn) is None]
        if missing:
            missing_by_variant.append(f"{variant_name}: {', '.join(sorted(missing))}")

    if missing_by_variant:
        die(
            "selected variant(s) require unsupported PostgreSQL parameter(s): "
            + " | ".join(missing_by_variant)
        )


def dataset_contexts(resolved_runs: list[ResolvedDatasetRun]) -> list[dict[str, Any]]:
    return [
        {
            "dataset": spec.dataset,
            "max_join": spec.max_join,
            "variants": list(spec.variants),
        }
        for spec in resolved_runs
    ]


def load_raw_rows(raw_path: Path) -> list[dict[str, str]]:
    if not raw_path.is_file():
        return []
    with raw_path.open(newline="") as f:
        return list(csv.DictReader(f))


def build_summary_acc_from_raw_rows(raw_rows: list[dict[str, str]]) -> dict[tuple[str, str, str], list[dict[str, object]]]:
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    for row in raw_rows:
        key = (row["dataset"], row["query_id"], row["variant"])
        summary_acc.setdefault(key, []).append(
            {
                "rep": int(row["rep"]),
                "planning_ms": float(row["planning_ms"]) if row["planning_ms"] else -1.0,
                "total_ms": float(row["total_ms"]) if row["total_ms"] else -1.0,
                "execution_ms": float(row["execution_ms"]) if row["execution_ms"] else -1.0,
                "execution_measurement_mode": row["execution_measurement_mode"],
                "plan_total_cost": float(row["plan_total_cost"]) if row["plan_total_cost"] else -1.0,
                "status": row["status"],
            }
        )
    return summary_acc


def serialize_warmup_groups(groups: set[tuple[int, str, str]]) -> list[dict[str, object]]:
    return [
        {"warmup_pass": warmup_pass, "dataset": dataset, "query_id": query_id}
        for warmup_pass, dataset, query_id in sorted(groups)
    ]


def deserialize_warmup_groups(items: list[dict[str, object]]) -> set[tuple[int, str, str]]:
    return {
        (int(item["warmup_pass"]), str(item["dataset"]), str(item["query_id"]))
        for item in items
    }


def serialize_measured_groups(groups: set[tuple[str, str, int]]) -> list[dict[str, object]]:
    return [
        {"dataset": dataset, "query_id": query_id, "rep": rep}
        for dataset, query_id, rep in sorted(groups)
    ]


def deserialize_measured_groups(items: list[dict[str, object]]) -> set[tuple[str, str, int]]:
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
    if run_context.get("scenario") != scenario.name:
        die(f"resume run_id belongs to scenario '{run_context.get('scenario')}', expected '{scenario.name}'")

    if (run_context.get("tag") or "") != tag:
        die(f"resume run_id has tag '{run_context.get('tag', '')}', expected '{tag}'")

    existing_timeout = run_context.get("statement_timeout_ms")
    if existing_timeout is None:
        existing_timeout = run_context.get("protocol", {}).get("statement_timeout_ms")
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
    raw_path = out_dir / "raw.csv"
    write_raw_csv(raw_path, raw_rows)

    summary_path = out_dir / "summary.csv"
    write_summary_csv(
        summary_path,
        run_id=run_id,
        scenario_name=scenario.name,
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


def run_scenario(
    scenario: Scenario,
    variants_registry: dict[str, Variant],
    variant_names: tuple[str, ...],
    resolved_runs: list[ResolvedDatasetRun],
    *,
    conn: Optional[ConnOpts],
    reps: int,
    statement_timeout_ms: int,
    stabilize: str,
    variant_order_mode: str,
    warmup_runs: int,
    resume_run_id: Optional[str],
    tag: str,
    fail_on_error: bool,
) -> None:
    if reps <= 0:
        die(f"scenario repetitions must be >= 1 (got {reps})")
    if statement_timeout_ms < 0:
        die(f"statement timeout must be >= 0 (got {statement_timeout_ms})")
    if warmup_runs < 0:
        die(f"warmup runs must be >= 0 (got {warmup_runs})")
    if variant_order_mode not in {"fixed", "rotate"}:
        die(f"scenario defines unsupported variant order mode: {variant_order_mode}")

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
            "session_gucs": [{k: v} for k, v in resolved_variant_session_gucs(dbs[0], conn, variants_registry[name])],
        }
        for name in variant_names
    ]

    print(f"[run] scenario={scenario.name}")
    print(f"[run] variants={','.join(variant_names)}")
    print(f"[run] warmup_passes={warmup_runs} measured_reps={reps}")
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

    for spec in resolved_runs:
        if spec.db not in stabilized_dbs:
            stabilize_db(spec.db, stabilize, conn)
            stabilized_dbs.add(spec.db)

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

    if resume_run_id:
        assert out_dir is not None
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
        raw_rows = load_raw_rows(out_dir / "raw.csv")
        summary_acc = build_summary_acc_from_raw_rows(raw_rows)
        warmup_failures = list(run_context.get("warmup_failures", []))
        warmup_timeout_keys = {
            (str(row["dataset"]), str(row["query_id"]), str(row["variant"]))
            for row in warmup_failures
            if row.get("category") == "statement_timeout"
        }
        progress = run_context.get("progress", {})
        completed_warmup_groups = deserialize_warmup_groups(progress.get("completed_warmup_groups", []))
        completed_measured_groups = deserialize_measured_groups(progress.get("completed_measured_groups", []))
        print(
            f"[run] resume_run_id={run_id} completed_warmup_groups={len(completed_warmup_groups)} "
            f"completed_measured_groups={len(completed_measured_groups)}"
        )

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

    for prepared in prepared_runs:
        if termination is not None:
            break
        spec = prepared["spec"]
        entry_variants = prepared["entry_variants"]
        query_plans = prepared["query_plans"]

        for query_idx, (q, stmt) in enumerate(query_plans):
            if termination is not None:
                break

            for warmup_pass in range(1, warmup_runs + 1):
                warmup_group = (warmup_pass, spec.dataset, q.query_id)
                if warmup_group in completed_warmup_groups:
                    continue
                ordered_variants = (
                    rotate_variants(entry_variants, query_idx + warmup_pass - 1)
                    if variant_order_mode == "rotate"
                    else list(entry_variants)
                )
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
                        warmup_timeout_keys.add((spec.dataset, q.query_id, variant.name))
                        record_warmup_failure(
                            warmup_failures=warmup_failures,
                            warmup_pass=warmup_pass,
                            spec=spec,
                            variant=variant,
                            query=q,
                            error=str(e),
                            category="statement_timeout",
                        )
                    except Exception as e:
                        record_warmup_failure(
                            warmup_failures=warmup_failures,
                            warmup_pass=warmup_pass,
                            spec=spec,
                            variant=variant,
                            query=q,
                            error=str(e),
                            category="error",
                        )
                        if fail_on_error:
                            termination = {
                                "phase": "warmup",
                                "category": "error",
                                "dataset": spec.dataset,
                                "db": spec.db,
                                "variant": variant.name,
                                "query_id": q.query_id,
                                "error": str(e),
                            }
                            print("[run] fail_on_error triggered during warmup; measured runs will be skipped")
                            break
                if termination is not None:
                    break
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

            for rep in range(1, reps + 1):
                measured_group = (spec.dataset, q.query_id, rep)
                if measured_group in completed_measured_groups:
                    continue
                ordered_variants = (
                    rotate_variants(entry_variants, query_idx + rep - 1)
                    if variant_order_mode == "rotate"
                    else list(entry_variants)
                )

                for variant_pos, variant in enumerate(ordered_variants, start=1):
                    key = (spec.dataset, q.query_id, variant.name)
                    status = "ok"
                    err = ""
                    planning_ms = -1.0
                    total_ms = -1.0
                    exec_ms = -1.0
                    plan_total_cost = -1.0
                    execution_measurement_mode = "explain_analyze_summary_timing_off_json"

                    if key in warmup_timeout_keys:
                        status = "timeout"
                        err = warmup_timeout_skip_error(
                            "ERROR: canceling statement due to statement timeout"
                        )
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
                            "run_id": run_id,
                            "scenario": scenario.name,
                            "dataset": spec.dataset,
                            "db": spec.db,
                            "variant": variant.name,
                            "query_id": q.query_id,
                            "query_label": q.query_label,
                            "query_path": q.query_path,
                            "join_size": str(q.join_size),
                            "rep": str(rep),
                            "variant_position": str(variant_pos),
                            "planning_ms": f"{planning_ms:.3f}" if planning_ms >= 0 else "",
                            "total_ms": f"{total_ms:.3f}" if total_ms >= 0 else "",
                            "execution_ms": f"{exec_ms:.3f}" if exec_ms >= 0 else "",
                            "execution_measurement_mode": execution_measurement_mode if status == "ok" else "",
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
                            "execution_measurement_mode": execution_measurement_mode,
                            "plan_total_cost": plan_total_cost,
                            "status": status,
                        }
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
            f"variant={termination['variant']} query={termination['query_id']}: {termination['error']}"
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
