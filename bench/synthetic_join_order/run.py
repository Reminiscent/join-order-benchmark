"""Plan and score one synthetic case across algorithm variants.

``run_variants()`` is the per-case unit used by the matrix runner. It plans and
qualifies the same compiled case under each variant, classifies each outcome as
``ok``, ``timeout``, or ``error``, and computes Total Cost Q among successful
plans. Matrix sizing, installation, persistence, and aggregation live in
``matrix.py`` and ``report.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bench_common import ConnOpts, Variant
from .compile import CompiledCase
from .plan import PlanDriverError, PlanEvidence, PlanTimeoutError, plan_case
from .qualify import QualificationError, qualify_postgres


@dataclass(frozen=True)
class VariantResult:
    """One algorithm's qualified outcome and optional PostgreSQL evidence."""

    instance_id: str
    variant: str
    status: str
    total_cost: float | None
    quality_ratio: float | None
    best_cost: float | None
    best_variant: str | None
    error: str
    evidence: PlanEvidence | None


def run_variants(
    db: str,
    graph: Mapping[str, Any],
    case: CompiledCase,
    variants: tuple[Variant, ...],
    conn: ConnOpts | None = None,
    *,
    statement_timeout_ms: int = 60_000,
    audit_mode: str = "summary",
) -> tuple[VariantResult, ...]:
    """Plan and qualify one case for each algorithm variant."""
    if not variants:
        raise ValueError("at least one variant is required")
    pending: list[VariantResult] = []
    for variant in variants:
        evidence = None
        try:
            evidence = plan_case(
                db,
                case,
                conn,
                statement_timeout_ms=statement_timeout_ms,
                variant_gucs=variant.session_gucs,
                audit_mode=audit_mode,
            )
            report = qualify_postgres(graph, case, evidence)
            pending.append(
                VariantResult(
                    instance_id=case.instance_id,
                    variant=variant.name,
                    status="ok",
                    total_cost=report.root_total_cost,
                    quality_ratio=None,
                    best_cost=None,
                    best_variant=None,
                    error="",
                    evidence=evidence,
                )
            )
        except PlanTimeoutError as exc:
            pending.append(
                VariantResult(
                    case.instance_id,
                    variant.name,
                    "timeout",
                    None,
                    None,
                    None,
                    None,
                    str(exc),
                    evidence,
                )
            )
        except (PlanDriverError, QualificationError) as exc:
            pending.append(
                VariantResult(
                    case.instance_id,
                    variant.name,
                    "error",
                    None,
                    None,
                    None,
                    None,
                    str(exc),
                    evidence,
                )
            )

    successful = [result for result in pending if result.status == "ok"]
    if not successful:
        return tuple(pending)
    best = min(successful, key=lambda result: (float(result.total_cost), result.variant))
    best_cost = float(best.total_cost)
    return tuple(
        VariantResult(
            instance_id=result.instance_id,
            variant=result.variant,
            status=result.status,
            total_cost=result.total_cost,
            quality_ratio=(float(result.total_cost) / best_cost if result.status == "ok" else None),
            best_cost=best_cost,
            best_variant=best.variant,
            error=result.error,
            evidence=result.evidence,
        )
        for result in pending
    )


def validate_run_results(results: tuple[VariantResult, ...]) -> None:
    """Fail on correctness errors while treating timeouts as protocol results."""
    errors = tuple(result for result in results if result.status == "error")
    if errors:
        details = "; ".join(f"{result.variant}: {result.error}" for result in errors)
        raise QualificationError(f"variant error: {details}")


def count_dp_anomalies(results: tuple[VariantResult, ...]) -> int:
    """Count successful results whose raw Total Cost is below DP."""
    successful = tuple(result for result in results if result.status == "ok")
    dp = next((result for result in successful if result.variant == "dp"), None)
    if dp is None:
        return 0
    dp_cost = float(dp.total_cost)
    return sum(
        result.variant != "dp" and float(result.total_cost) < dp_cost
        for result in successful
    )
