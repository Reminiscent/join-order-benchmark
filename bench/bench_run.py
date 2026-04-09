from __future__ import annotations

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
    safe_artifact_name,
    utc_now,
)
from bench_environment import ensure_databases_reachable, resolved_variant_session_gucs, validate_required_gucs
from bench_exec import (
    rotate_variants,
    run_one,
    stabilize_db,
)
from bench_public_report import write_public_reports
from bench_results import build_run_context, write_raw_csv, write_run_context, write_summary_csv


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
    tag: str,
    fail_on_error: bool,
) -> None:
    if reps <= 0:
        die(f"--reps must be >= 1 (got {reps})")
    if statement_timeout_ms < 0:
        die(f"--statement-timeout-ms must be >= 0 (got {statement_timeout_ms})")
    if warmup_runs < 0:
        die(f"--warmup-runs must be >= 0 (got {warmup_runs})")
    if variant_order_mode not in {"fixed", "rotate"}:
        die(f"scenario defines unsupported variant order mode: {variant_order_mode}")

    persist_outputs = scenario.name != "smoke"
    run_id = f"{utc_now().strftime('%Y%m%d_%H%M%S_%f')}_{safe_artifact_name(scenario.name)}" if persist_outputs else ""
    out_dir: Path | None = None
    if persist_outputs:
        OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
        out_dir = OUTPUTS_DIR / run_id
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
    if persist_outputs:
        assert out_dir is not None
        print(f"[run] outputs={out_dir}")
    else:
        print("[run] smoke mode: no outputs will be written")

    raw_rows: list[dict[str, str]] = []
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    query_counts: list[dict[str, Any]] = []
    stabilized_dbs: set[str] = set()
    prepared_runs: list[dict[str, Any]] = []

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
                "min_join": spec.min_join,
                "max_join": spec.max_join,
                "max_queries": spec.max_queries,
                "queries_selected": len(query_plans),
                "variants": list(spec.variants),
            }
        )
        print(
            f"[run] dataset={spec.dataset} db={spec.db} queries={len(query_plans)} "
            f"variants={','.join(spec.variants)} min_join={spec.min_join} max_join={spec.max_join}"
        )

        entry_variants = [variants_registry[name] for name in spec.variants]
        prepared_runs.append(
            {
                "spec": spec,
                "entry_variants": entry_variants,
                "query_plans": query_plans,
            }
        )

    if warmup_runs > 0:
        for warmup_pass in range(1, warmup_runs + 1):
            print(f"[run] warmup_pass={warmup_pass}/{warmup_runs}")
            for prepared in prepared_runs:
                spec = prepared["spec"]
                entry_variants = prepared["entry_variants"]
                query_plans = prepared["query_plans"]
                for query_idx, (q, stmt) in enumerate(query_plans):
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
                        except Exception as e:
                            print(
                                f"[run] warmup_error dataset={spec.dataset} variant={variant.name} "
                                f"query={q.query_id}: {e}"
                            )
                            if fail_on_error:
                                raise SystemExit(1)

    for prepared in prepared_runs:
        spec = prepared["spec"]
        entry_variants = prepared["entry_variants"]
        query_plans = prepared["query_plans"]

        for query_idx, (q, stmt) in enumerate(query_plans):

            for rep in range(1, reps + 1):
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

    if persist_outputs:
        assert out_dir is not None
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
            reps=reps,
            statement_timeout_ms=statement_timeout_ms,
            stabilize=stabilize,
            warmup_runs=warmup_runs,
            effective_variant_contexts=effective_variant_contexts,
            query_counts=query_counts,
        )

        public_report_markdown_path = out_dir / "public_report.md"
        public_report_json_path = out_dir / "public_report.json"
        write_public_reports(
            run_context=run_context,
            summary_path=summary_path,
            markdown_path=public_report_markdown_path,
            json_path=public_report_json_path,
        )
        print(f"[run] public_report_markdown={public_report_markdown_path}")
        print(f"[run] public_report_json={public_report_json_path}")

        write_run_context(out_dir / "run.json", run_context)

    err_rows = [row for row in raw_rows if row["status"] == "error"]
    if err_rows:
        print(f"[run] errors={len(err_rows)}")
        for row in err_rows[:5]:
            print(
                f"[run] error dataset={row['dataset']} variant={row['variant']} "
                f"query={row['query_id']}: {row['error']}"
            )
        if len(err_rows) > 5:
            print(f"[run] ... and {len(err_rows) - 5} more errors")
        if fail_on_error:
            raise SystemExit(1)
    else:
        print("[run] completed without errors")
