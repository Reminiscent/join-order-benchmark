"""Orchestrate the deterministic size/seed matrix and durable artifacts.

For every ``GenerationSpec``, ``run_matrix()`` generates and installs one case,
delegates per-algorithm planning to ``run.py``, writes one authoritative
``quality.csv`` row per result, records compact workload diagnostics, and
always attempts cleanup. Correctness errors abort with failure artifacts;
timeouts remain coverage results. ``report.py`` is a separate reader of these
outputs.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping

from bench_common import ConnOpts, Variant, safe_artifact_name
from .compile import VIRTUAL_PAGE_BYTES, CompiledCase, compile_graph, write_case
from .graph import (
    BASE_ROWS_MAX,
    BASE_ROWS_MIN,
    CARDINALITY_BUDGET,
    GenerationSpec,
    generate_graph,
)
from .cardinality import (
    LOG_MAXIMUM_ROWCOUNT,
    MAXIMUM_ROWCOUNT,
    clamp_row_est_from_log,
    subset_log_cardinality,
)
from .plan import (
    BENCHMARK_GUCS,
    FORMULA_PATH_COST_FUZZ_FACTOR,
    PlanDriverError,
    install_case,
    uninstall_case,
)
from .qualify import QualificationError, selected_plan_join_diagnostics
from .run import (
    VariantResult,
    count_dp_anomalies,
    run_variants,
    validate_run_results,
)


MATRIX_QUALITY_FIELDS = (
    "n",
    "graph_seed",
    "cardinality_seed",
    "instance_id",
    "variant",
    "status",
    "total_cost",
    "quality_ratio",
    "best_cost",
    "best_variant",
    "error",
)
WORKLOAD_DIAGNOSTIC_FIELDS = (
    "n",
    "graph_seed",
    "cardinality_seed",
    "instance_id",
    "variant",
    "status",
    "anchor_relation",
    "cardinality_budget",
    "root_growth_factor",
    "selective_edge_count",
    "neutral_edge_count",
    "expanding_edge_count",
    "contraction_log_budget",
    "expansion_log_budget",
    "minimum_connected_log_rows",
    "maximum_connected_log_rows",
    "root_effective_log",
    "root_effective_clamped_rows",
    "root_clamp",
    "selected_intermediate_min_clamps",
    "selected_intermediate_max_clamps",
)
MAIN_SIZES = tuple(range(12, 21))
MAIN_INSTANCES = 100
MAIN_GRAPH_SEED_START = 200
MAIN_CARDINALITY_SEED_START = 1200
DP_MAX_SIZE = 20


def matrix_specs(
    sizes: tuple[int, ...],
    instance_count: int,
    *,
    graph_seed_start: int = MAIN_GRAPH_SEED_START,
    cardinality_seed_start: int = MAIN_CARDINALITY_SEED_START,
) -> tuple[GenerationSpec, ...]:
    """Expand the fixed seed contract for selected sizes."""
    if not sizes or any(size < 2 for size in sizes):
        raise ValueError("matrix sizes must contain relation counts >= 2")
    if len(set(sizes)) != len(sizes):
        raise ValueError("matrix sizes must not contain duplicates")
    if instance_count <= 0:
        raise ValueError("instance_count must be positive")
    if graph_seed_start < 0 or cardinality_seed_start < 0:
        raise ValueError("seed starts must be non-negative")
    return tuple(
        GenerationSpec(
            n=size,
            graph_seed=graph_seed_start + index,
            cardinality_seed=cardinality_seed_start + index,
        )
        for size in sizes
        for index in range(instance_count)
    )


def variants_for_size(variants: tuple[Variant, ...], size: int) -> tuple[Variant, ...]:
    """Return the requested variants eligible at one matrix size."""
    return tuple(variant for variant in variants if variant.name != "dp" or size <= DP_MAX_SIZE)


def partition_matrix_sizes(
    sizes: tuple[int, ...], variants: tuple[Variant, ...]
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Separate comparable sizes from sizes with fewer than two algorithms."""
    runnable = tuple(
        size for size in sizes if len(variants_for_size(variants, size)) >= 2
    )
    skipped = tuple(size for size in sizes if size not in runnable)
    return runnable, skipped


def validate_matrix_request(
    sizes: tuple[int, ...],
    instance_count: int,
    variants: tuple[Variant, ...],
    output_dir: Path,
    *,
    statement_timeout_ms: int,
    graph_seed_start: int,
    cardinality_seed_start: int,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Validate a request and return its runnable and intentionally skipped sizes."""
    if statement_timeout_ms <= 0:
        raise ValueError("statement_timeout_ms must be positive")
    variant_names = tuple(variant.name for variant in variants)
    if len(set(variant_names)) != len(variant_names):
        raise ValueError("matrix variants must not contain duplicates")
    matrix_specs(
        sizes,
        instance_count,
        graph_seed_start=graph_seed_start,
        cardinality_seed_start=cardinality_seed_start,
    )
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError(f"matrix output directory is not empty: {output_dir}")
    runnable_sizes, skipped_sizes = partition_matrix_sizes(sizes, variants)
    if not runnable_sizes:
        details = ", ".join(
            f"{size} ({', '.join(variant.name for variant in variants_for_size(variants, size)) or 'none'})"
            for size in skipped_sizes
        )
        raise ValueError(
            "every requested matrix size has fewer than two eligible variants: "
            + details
        )
    return runnable_sizes, skipped_sizes


def run_matrix(
    db: str,
    sizes: tuple[int, ...],
    instance_count: int,
    variants: tuple[Variant, ...],
    output_dir: Path,
    conn: ConnOpts | None = None,
    *,
    statement_timeout_ms: int = 60_000,
    graph_seed_start: int = MAIN_GRAPH_SEED_START,
    cardinality_seed_start: int = MAIN_CARDINALITY_SEED_START,
    benchmark_metadata: Mapping[str, Any] | None = None,
    postgres_metadata: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], ...]:
    """Generate, install, qualify, and score every selected matrix instance."""
    # Resolve the population before creating output files or touching the DB.
    requested_sizes = sizes
    sizes, skipped_sizes = validate_matrix_request(
        sizes,
        instance_count,
        variants,
        output_dir,
        statement_timeout_ms=statement_timeout_ms,
        graph_seed_start=graph_seed_start,
        cardinality_seed_start=cardinality_seed_start,
    )
    specs = matrix_specs(
        sizes,
        instance_count,
        graph_seed_start=graph_seed_start,
        cardinality_seed_start=cardinality_seed_start,
    )
    variants_by_size = {
        size: variants_for_size(variants, size) for size in sizes
    }

    # Write an incomplete run record first so an interrupted run remains
    # diagnosable rather than looking complete.
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    anomaly_count = 0
    context = {
        "format": "synthetic-join-order-matrix",
        "benchmark": dict(benchmark_metadata or {}),
        "postgres": dict(postgres_metadata or {}),
        "requested_sizes": list(requested_sizes),
        "sizes": list(sizes),
        "skipped_sizes": [
            {
                "n": size,
                "eligible_variants": [
                    variant.name for variant in variants_for_size(variants, size)
                ],
                "reason": "fewer than two eligible variants",
            }
            for size in skipped_sizes
        ],
        "instance_count_per_size": instance_count,
        "seed_contract": {
            "graph_seed_start": graph_seed_start,
            "graph_seed_end": graph_seed_start + instance_count - 1,
            "cardinality_seed_start": cardinality_seed_start,
            "cardinality_seed_end": cardinality_seed_start + instance_count - 1,
        },
        "cardinality_bounds": {
            "base_rows_min": BASE_ROWS_MIN,
            "base_rows_max": BASE_ROWS_MAX,
            "cardinality_budget": CARDINALITY_BUDGET,
        },
        "virtual_page_bytes": VIRTUAL_PAGE_BYTES,
        "benchmark_gucs": [{name: value} for name, value in BENCHMARK_GUCS],
        "formula_path_cost_fuzz_factor": FORMULA_PATH_COST_FUZZ_FACTOR,
        "variants": [variant.name for variant in variants],
        "variant_gucs": {
            variant.name: [
                {name: value} for name, value in variant.session_gucs
            ]
            for variant in variants
        },
        "variant_policy": {"dp_max_size": DP_MAX_SIZE},
        "scheduled_variants_by_size": {
            str(size): [variant.name for variant in variants_by_size[size]] for size in sizes
        },
        "statement_timeout_ms": statement_timeout_ms,
        "audit_mode": "summary",
        "maximum_rowcount": MAXIMUM_ROWCOUNT,
        "instances": len(specs),
        "processed_instances": 0,
        "complete": False,
        "abort_error": None,
        "status_counts": {"ok": 0, "timeout": 0, "error": 0},
        "dp_anomaly_count": 0,
    }
    _write_json(output_dir / "run.json", context)

    with (
        (output_dir / "quality.csv").open("w", newline="") as quality_file,
        (output_dir / "workload_diagnostics.csv").open("w", newline="") as diagnostics_file,
    ):
        quality_writer = csv.DictWriter(
            quality_file, fieldnames=MATRIX_QUALITY_FIELDS, lineterminator="\n"
        )
        diagnostics_writer = csv.DictWriter(
            diagnostics_file,
            fieldnames=WORKLOAD_DIAGNOSTIC_FIELDS,
            lineterminator="\n",
        )
        quality_writer.writeheader()
        diagnostics_writer.writeheader()
        for spec in specs:
            # One case is installed, compared across all eligible variants,
            # persisted, and removed before advancing to the next seed.
            graph = generate_graph(spec)
            case = compile_graph(graph)
            label = f"n{spec.n:04d}_g{spec.graph_seed:04d}_c{spec.cardinality_seed:04d}"
            try:
                install_case(db, case, conn)
            except PlanDriverError as exc:
                write_case(case, output_dir / "failures" / label / "case")
                context["abort_error"] = str(exc)
                _write_json(output_dir / "run.json", context)
                raise
            primary_error: BaseException | None = None
            try:
                selected_variants = variants_by_size[spec.n]
                results = run_variants(
                    db,
                    graph,
                    case,
                    selected_variants,
                    conn,
                    statement_timeout_ms=statement_timeout_ms,
                    audit_mode="summary",
                )
                case_rows = [_matrix_row(spec, result) for result in results]
                rows.extend(case_rows)
                quality_writer.writerows(case_rows)
                quality_file.flush()
                diagnostics_writer.writerows(
                    _workload_diagnostic_row(spec, graph, result) for result in results
                )
                diagnostics_file.flush()

                # Seeds identify the case.  run.json adds the declared source
                # revision, live server version, and variant settings as review context.
                anomaly_count += count_dp_anomalies(results)

                for result in results:
                    context["status_counts"][result.status] += 1
                context["processed_instances"] += 1
                context["dp_anomaly_count"] = anomaly_count

                if any(result.status == "error" for result in results):
                    _write_failure_artifacts(
                        output_dir / "failures" / label, case, results
                    )
                    context["abort_error"] = "; ".join(
                        f"{result.variant}: {result.error}"
                        for result in results
                        if result.status == "error"
                    )
                _write_json(output_dir / "run.json", context)
                validate_run_results(results)
            except BaseException as exc:
                primary_error = exc
                if not context["abort_error"]:
                    context["abort_error"] = str(exc)
                    _write_json(output_dir / "run.json", context)
                raise
            finally:
                try:
                    uninstall_case(db, case, conn)
                except PlanDriverError as exc:
                    cleanup_error = str(exc)
                    suffix = f"case cleanup failed: {cleanup_error}"
                    if context["abort_error"]:
                        context["abort_error"] += f"; {suffix}"
                    else:
                        context["abort_error"] = suffix
                    _write_json(output_dir / "run.json", context)
                    if primary_error is None:
                        raise
                    primary_error.add_note(suffix)

    # Mark completion only after every case has been persisted and cleaned up.
    context["dp_anomaly_count"] = anomaly_count
    context["complete"] = True
    _write_json(output_dir / "run.json", context)
    return tuple(rows)


def _spec_fields(spec: GenerationSpec) -> dict[str, int]:
    return {
        "n": spec.n,
        "graph_seed": spec.graph_seed,
        "cardinality_seed": spec.cardinality_seed,
    }


def _matrix_row(spec: GenerationSpec, result: VariantResult) -> dict[str, Any]:
    return {
        **_spec_fields(spec),
        "instance_id": result.instance_id,
        "variant": result.variant,
        "status": result.status,
        "total_cost": "" if result.total_cost is None else result.total_cost,
        "quality_ratio": "" if result.quality_ratio is None else result.quality_ratio,
        "best_cost": "" if result.best_cost is None else result.best_cost,
        "best_variant": "" if result.best_variant is None else result.best_variant,
        "error": result.error,
    }


def _workload_diagnostic_row(
    spec: GenerationSpec, graph: dict[str, Any], result: VariantResult
) -> dict[str, Any]:
    model = graph["cardinality_model"]
    effective_root_log = subset_log_cardinality(
        graph, tuple(range(len(graph["nodes"])))
    )
    if effective_root_log <= 0.0:
        root_clamp = "min"
    elif effective_root_log >= LOG_MAXIMUM_ROWCOUNT:
        root_clamp = "max"
    else:
        root_clamp = "none"

    plan_values: dict[str, Any] = {
        "selected_intermediate_min_clamps": "",
        "selected_intermediate_max_clamps": "",
    }
    if result.status == "ok" and result.evidence is not None:
        intermediate_logs = selected_plan_join_diagnostics(
            graph, result.evidence.explain["Plan"]
        )
        plan_values = {
            "selected_intermediate_min_clamps": sum(
                value <= 0.0 for value in intermediate_logs
            ),
            "selected_intermediate_max_clamps": sum(
                value >= LOG_MAXIMUM_ROWCOUNT for value in intermediate_logs
            ),
        }

    return {
        **_spec_fields(spec),
        "instance_id": result.instance_id,
        "variant": result.variant,
        "status": result.status,
        "anchor_relation": model["anchor_relation"],
        "cardinality_budget": model["budget"],
        "root_growth_factor": model["root_growth_factor"],
        "selective_edge_count": model["selective_edge_count"],
        "neutral_edge_count": model["neutral_edge_count"],
        "expanding_edge_count": model["expanding_edge_count"],
        "contraction_log_budget": model["contraction_log_budget"],
        "expansion_log_budget": model["expansion_log_budget"],
        "minimum_connected_log_rows": model["minimum_connected_log_rows"],
        "maximum_connected_log_rows": model["maximum_connected_log_rows"],
        "root_effective_log": effective_root_log,
        "root_effective_clamped_rows": clamp_row_est_from_log(effective_root_log),
        "root_clamp": root_clamp,
        **plan_values,
    }


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _write_failure_artifacts(
    output_dir: Path,
    case: CompiledCase,
    results: tuple[VariantResult, ...],
) -> None:
    write_case(case, output_dir / "case")
    for result in results:
        if result.evidence is None:
            continue
        name = safe_artifact_name(result.variant)
        _write_json(output_dir / f"{name}_plan.json", result.evidence.explain)
        _write_json(output_dir / f"{name}_summary.json", result.evidence.audit)
