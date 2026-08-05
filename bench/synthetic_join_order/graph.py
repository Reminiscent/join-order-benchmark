"""Build the deterministic graph IR used by the synthetic join-order workload.

Reading path:

1. ``GenerationSpec`` supplies a relation count and independent topology and
   cardinality seeds.
2. ``generate_graph`` decodes a random Prüfer sequence into a tree, samples
   base rows and edge key behavior, and applies one graph-wide root-balance
   scale.
3. The returned graph stores the final selectivities consumed by
   ``cardinality.py`` and ``compile.py``.
4. ``validate_graph`` checks structure and recomputes every derived value.

Only topology, base rows, edge families, initial key overlap, and endpoint key
multiplicities are random. Integer endpoint key NDVs, effective key overlap,
selectivities, root balance, and the instance digest are derived.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Any, Iterable, Mapping


# Seeds and PRNG state are unsigned 64-bit values.
MASK64 = (1 << 64) - 1


class SpecError(ValueError):
    """Raised when a generation spec or expanded graph violates the contract."""


@dataclass(frozen=True)
class GenerationSpec:
    """The only user-controlled inputs needed to reproduce one graph."""

    n: int
    graph_seed: int
    cardinality_seed: int

    def __post_init__(self) -> None:
        integer_fields = {
            "n": self.n,
            "graph_seed": self.graph_seed,
            "cardinality_seed": self.cardinality_seed,
        }
        for name, value in integer_fields.items():
            if not isinstance(value, int) or isinstance(value, bool):
                raise SpecError(f"{name} must be an integer")
        if not 2 <= self.n <= 5000:
            raise SpecError("n must be between 2 and 5000")
        for name in ("graph_seed", "cardinality_seed"):
            if not 0 <= integer_fields[name] <= MASK64:
                raise SpecError(f"{name} must fit in an unsigned 64-bit integer")

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "GenerationSpec":
        expected = {"n", "graph_seed", "cardinality_seed"}
        missing = expected - set(raw)
        unknown = set(raw) - expected
        if missing:
            raise SpecError(f"missing spec fields: {', '.join(sorted(missing))}")
        if unknown:
            raise SpecError(f"unknown spec fields: {', '.join(sorted(unknown))}")
        return cls(**{name: raw[name] for name in expected})

    def to_dict(self) -> dict[str, Any]:
        return {
            "n": self.n,
            "graph_seed": self.graph_seed,
            "cardinality_seed": self.cardinality_seed,
        }


# Expanded graph identity.
GRAPH_FORMAT = "synthetic-join-order-graph"

# Base relation rows are sampled log-uniformly across three decades, so small
# and large orders of magnitude both occur regularly.
BASE_ROWS_MIN = 1_000
BASE_ROWS_MAX = 1_000_000

# Root balance limits geometric average growth per added relation. It scales
# every edge equally and therefore preserves their relative selectivities.
MAX_AVERAGE_JOIN_GROWTH = 1.20

# Four of five edges are one-to-many; the remaining one is many-to-many.
PROBABILITY_DENOMINATOR = 5
ONE_TO_MANY_NUMERATOR = 4

# One-to-many edges have one-in-five full overlaps. Other overlap fractions
# are sampled log-uniformly from 0.1 to 1.
FULL_MATCH_NUMERATOR = 1
ONE_TO_MANY_OVERLAP_MIN = 0.1

# Endpoint multiplicities vary by up to this factor beyond the minimum needed
# to keep both endpoint key domains within the smaller relation's row count.
# This keeps overlap meaningful while allowing asymmetric endpoint NDVs.
ENDPOINT_MULTIPLICITY_SPREAD_MAX = 8.0

# Many-to-many edges independently vary effective key overlap and endpoint
# multiplicity. Together they can produce either contraction or expansion.
MANY_TO_MANY_OVERLAP_MIN = 0.03


class SplitMix64:
    """Small fixed PRNG with behavior independent of Python's runtime RNG."""

    def __init__(self, seed: int) -> None:
        if not isinstance(seed, int) or isinstance(seed, bool):
            raise SpecError("seed must be an integer")
        if not 0 <= seed <= MASK64:
            raise SpecError("seed must fit in an unsigned 64-bit integer")
        self._state = seed

    def next_u64(self) -> int:
        self._state = (self._state + 0x9E3779B97F4A7C15) & MASK64
        value = self._state
        value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
        value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
        return (value ^ (value >> 31)) & MASK64

    def randbelow(self, bound: int) -> int:
        """Return an unbiased integer in ``[0, bound)`` using rejection sampling."""
        if not isinstance(bound, int) or isinstance(bound, bool) or bound <= 0:
            raise SpecError("randbelow bound must be a positive integer")
        limit = (1 << 64) - ((1 << 64) % bound)
        while True:
            value = self.next_u64()
            if value < limit:
                return value % bound


def canonical_json(value: Any) -> str:
    """Serialize canonical JSON; the terminating newline is part of the digest."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def graph_instance_id(graph: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(graph).encode("utf-8")).hexdigest()


def edge_id(left: int, right: int) -> str:
    left, right = sorted((left, right))
    return f"e_{left:04d}_{right:04d}"


def decode_prufer(sequence: Iterable[int], n: int) -> tuple[tuple[int, int], ...]:
    """Decode an ``n - 2`` item Prüfer sequence into one labeled tree.

    A node starts with degree one plus its occurrence count in the sequence.
    For each sequence item, connect it to the smallest current leaf and lower
    both degrees. The two remaining leaves form the final edge.
    """
    sequence = tuple(sequence)
    if n < 2 or len(sequence) != n - 2:
        raise SpecError("a Prüfer sequence for n nodes must have length n - 2")
    if any(not isinstance(node, int) or isinstance(node, bool) or not 0 <= node < n for node in sequence):
        raise SpecError("Prüfer sequence contains an invalid node id")

    degree = [1] * n
    for node in sequence:
        degree[node] += 1
    leaves = [node for node, value in enumerate(degree) if value == 1]
    heapq.heapify(leaves)
    edges: list[tuple[int, int]] = []
    for node in sequence:
        leaf = heapq.heappop(leaves)
        edges.append(tuple(sorted((leaf, node))))
        degree[leaf] -= 1
        degree[node] -= 1
        if degree[node] == 1:
            heapq.heappush(leaves, node)
    final_left = heapq.heappop(leaves)
    final_right = heapq.heappop(leaves)
    edges.append(tuple(sorted((final_left, final_right))))
    return tuple(sorted(edges))


def _log_uniform_int(rng: SplitMix64, low: int, high: int) -> int:
    """Sample an integer uniformly in log space, then round half up.

    Equal logarithmic intervals receive equal probability, so each order of
    magnitude is represented rather than values clustering near ``high``.
    """
    if low == high:
        return low
    sample = rng.next_u64()
    with localcontext() as context:
        context.prec = 80
        fraction = Decimal(sample) / Decimal(1 << 64)
        low_log = Decimal(low).ln()
        high_log = Decimal(high).ln()
        value = (low_log + fraction * (high_log - low_log)).exp()
        rounded = int(value.to_integral_value(rounding=ROUND_HALF_UP))
    return min(high, max(low, rounded))


def _log_uniform_float(rng: SplitMix64, low: float, high: float) -> float:
    """Sample a float uniformly in log space within positive bounds."""
    if not 0.0 < low <= high:
        raise SpecError("log-uniform bounds must satisfy 0 < low <= high")
    if low == high:
        return low
    sample = rng.next_u64()
    with localcontext() as context:
        context.prec = 80
        fraction = Decimal(sample) / Decimal(1 << 64)
        low_decimal = Decimal(str(low))
        high_decimal = Decimal(str(high))
        value = (
            low_decimal.ln()
            + fraction * (high_decimal.ln() - low_decimal.ln())
        ).exp()
    result = float(value)
    if not math.isfinite(result) or not low <= result <= high:
        raise SpecError("log-uniform generation produced an invalid value")
    return result


def _ceil_positive_ratio(numerator: int, denominator: float) -> int:
    """Round a positive ratio up to a legal integer key NDV."""
    with localcontext() as context:
        context.prec = 80
        value = Decimal(numerator) / Decimal(str(denominator))
        return max(1, math.ceil(value))


def _sample_endpoint_key_ndv(
    rng: SplitMix64,
    endpoint_rows: int,
    domain_capacity: int,
    *,
    require_repeated: bool,
) -> int:
    """Sample endpoint multiplicity and return its integer key NDV.

    ``endpoint_rows / domain_capacity`` is the minimum multiplicity needed for
    this endpoint's NDV not to exceed the smaller endpoint's row count. A
    bounded multiplicative spread then varies repetition independently on each
    side while keeping the two key domains on a comparable scale.
    ``require_repeated`` keeps the result below the endpoint row count.
    """
    minimum_multiplicity = endpoint_rows / domain_capacity
    sampled_multiplicity = _log_uniform_float(
        rng,
        minimum_multiplicity,
        minimum_multiplicity * ENDPOINT_MULTIPLICITY_SPREAD_MAX,
    )
    key_ndv = min(
        domain_capacity,
        _ceil_positive_ratio(endpoint_rows, sampled_multiplicity),
    )
    if require_repeated:
        key_ndv = min(key_ndv, endpoint_rows - 1)
    return key_ndv


def edge_endpoint_key_ndv(edge: Mapping[str, Any], endpoint: int) -> int:
    """Return the key NDV modeled for one endpoint of an edge."""
    if endpoint == edge["left"]:
        return edge["left_key_ndv"]
    if endpoint == edge["right"]:
        return edge["right_key_ndv"]
    raise SpecError("relation is not an endpoint of the edge")


def generate_graph(spec: GenerationSpec) -> dict[str, Any]:
    """Expand one spec into the canonical graph IR.

    ``graph_seed`` controls only the Prüfer sequence and therefore tree shape.
    ``cardinality_seed`` controls base rows, edge family, initial key overlap,
    and endpoint key multiplicities. Integer endpoint key NDVs, effective key
    overlap, the root-balance scale, and selectivities are deterministic
    calculations.
    """
    # Step 1: one length-(n - 2) sequence identifies one labeled tree.
    graph_rng = SplitMix64(spec.graph_seed)
    sequence = tuple(graph_rng.randbelow(spec.n) for _ in range(spec.n - 2))
    bare_edges = decode_prufer(sequence, spec.n)

    # Step 2: sample base rows log-uniformly and independently of topology.
    cardinality_rng = SplitMix64(spec.cardinality_seed)
    base_rows = [
        _log_uniform_int(cardinality_rng, BASE_ROWS_MIN, BASE_ROWS_MAX)
        for _ in range(spec.n)
    ]
    nodes = [
        {"id": node_id, "base_rows": rows}
        for node_id, rows in enumerate(base_rows)
    ]

    # Step 3: sample initial overlap and independent endpoint key
    # multiplicities. One-to-many edges keep the smaller endpoint unique and
    # repeat keys on the other side; many-to-many edges repeat keys on both.
    edges: list[dict[str, Any]] = []
    for left, right in bare_edges:
        left_rows = base_rows[left]
        right_rows = base_rows[right]
        minimum_rows = min(left_rows, right_rows)
        is_one_to_many = (
            cardinality_rng.randbelow(PROBABILITY_DENOMINATOR)
            < ONE_TO_MANY_NUMERATOR
        )
        if is_one_to_many:
            if left_rows <= right_rows:
                left_key_ndv = left_rows
                right_key_ndv = _sample_endpoint_key_ndv(
                    cardinality_rng,
                    right_rows,
                    left_rows,
                    require_repeated=True,
                )
            else:
                left_key_ndv = _sample_endpoint_key_ndv(
                    cardinality_rng,
                    left_rows,
                    right_rows,
                    require_repeated=True,
                )
                right_key_ndv = right_rows
            if (
                cardinality_rng.randbelow(PROBABILITY_DENOMINATOR)
                < FULL_MATCH_NUMERATOR
            ):
                key_overlap_fraction = 1.0
            else:
                key_overlap_fraction = _log_uniform_float(
                    cardinality_rng, ONE_TO_MANY_OVERLAP_MIN, 1.0
                )
            edge = {
                "id": edge_id(left, right),
                "left": left,
                "right": right,
                "type": "one_to_many",
                "left_key_ndv": left_key_ndv,
                "right_key_ndv": right_key_ndv,
                "key_overlap_fraction": key_overlap_fraction,
            }
        else:
            left_key_ndv = _sample_endpoint_key_ndv(
                cardinality_rng,
                left_rows,
                minimum_rows,
                require_repeated=True,
            )
            right_key_ndv = _sample_endpoint_key_ndv(
                cardinality_rng,
                right_rows,
                minimum_rows,
                require_repeated=True,
            )
            key_overlap_fraction = _log_uniform_float(
                cardinality_rng, MANY_TO_MANY_OVERLAP_MIN, 1.0
            )
            edge = {
                "id": edge_id(left, right),
                "left": left,
                "right": right,
                "type": "many_to_many",
                "left_key_ndv": left_key_ndv,
                "right_key_ndv": right_key_ndv,
                "key_overlap_fraction": key_overlap_fraction,
            }
        edges.append(edge)

    # Step 4: apply one downward scale when the initial graph grows too
    # quickly. This preserves relative selectivities without resampling.
    base_log_sum = math.fsum(math.log(rows) for rows in base_rows)
    typical_base_log = base_log_sum / spec.n
    raw_selectivities = [
        edge["key_overlap_fraction"]
        / max(edge["left_key_ndv"], edge["right_key_ndv"])
        for edge in edges
    ]
    raw_root_log = base_log_sum + math.fsum(
        math.log(selectivity) for selectivity in raw_selectivities
    )
    raw_growth_log = (raw_root_log - typical_base_log) / (spec.n - 1)
    log_edge_scale = min(
        0.0, math.log(MAX_AVERAGE_JOIN_GROWTH) - raw_growth_log
    )
    edge_scale = math.exp(log_edge_scale)
    for edge in edges:
        # Store effective overlap, so the graph contract stays simply
        # selectivity = overlap / the larger endpoint NDV.
        edge["key_overlap_fraction"] *= edge_scale
        edge["selectivity"] = (
            edge["key_overlap_fraction"]
            / max(edge["left_key_ndv"], edge["right_key_ndv"])
        )

    graph = {
        "format": GRAPH_FORMAT,
        "source_spec": spec.to_dict(),
        "nodes": nodes,
        "edges": edges,
        "root_balance": {
            "raw_average_growth": math.exp(raw_growth_log),
            "edge_scale": edge_scale,
        },
    }
    validate_graph(graph)
    return graph


def validate_graph(graph: Mapping[str, Any]) -> None:
    """Reject malformed graphs and drift in regenerated or derived fields."""
    if graph.get("format") != GRAPH_FORMAT:
        raise SpecError("unsupported graph format")
    raw_spec = graph.get("source_spec")
    if not isinstance(raw_spec, Mapping):
        raise SpecError("source_spec must be an object")
    spec = GenerationSpec.from_dict(raw_spec)
    nodes = graph.get("nodes")
    edges = graph.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise SpecError("nodes and edges must be lists")
    if any(not isinstance(node, Mapping) for node in nodes):
        raise SpecError("every node must be an object")
    if [node.get("id") for node in nodes] != list(range(spec.n)):
        raise SpecError("nodes must be ordered by contiguous canonical id")
    if any(not isinstance(node.get("base_rows"), int) or node["base_rows"] < 1 for node in nodes):
        raise SpecError("every node must have positive integer base_rows")
    if any(
        not BASE_ROWS_MIN <= node["base_rows"] <= BASE_ROWS_MAX for node in nodes
    ):
        raise SpecError("node base_rows fall outside source_spec bounds")
    if len(edges) != spec.n - 1:
        raise SpecError("tree must have exactly n - 1 edges")
    if any(not isinstance(edge, Mapping) for edge in edges):
        raise SpecError("every edge must be an object")

    seen: set[tuple[int, int]] = set()
    adjacency = [[] for _ in range(spec.n)]
    previous_id = ""
    for edge in edges:
        left, right = edge.get("left"), edge.get("right")
        if not isinstance(left, int) or not isinstance(right, int):
            raise SpecError("edge endpoints must be integers")
        if not 0 <= left < right < spec.n:
            raise SpecError("edge endpoints must be canonical and in range")
        pair = (left, right)
        if pair in seen:
            raise SpecError("duplicate edge")
        seen.add(pair)
        expected_id = edge_id(left, right)
        if edge.get("id") != expected_id or expected_id <= previous_id:
            raise SpecError("edges must use canonical ids and sorted order")
        previous_id = expected_id
        left_rows = nodes[left]["base_rows"]
        right_rows = nodes[right]["base_rows"]
        minimum_rows = min(left_rows, right_rows)
        left_key_ndv = edge.get("left_key_ndv")
        right_key_ndv = edge.get("right_key_ndv")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in (left_key_ndv, right_key_ndv)
        ):
            raise SpecError("endpoint key NDVs must be positive integers")
        if left_key_ndv > left_rows or right_key_ndv > right_rows:
            raise SpecError("endpoint key NDV exceeds its relation cardinality")
        if max(left_key_ndv, right_key_ndv) > minimum_rows:
            raise SpecError("endpoint key domains exceed the shared capacity")
        key_overlap_fraction = edge.get("key_overlap_fraction")
        selectivity = edge.get("selectivity")
        if (
            not isinstance(key_overlap_fraction, float)
            or not math.isfinite(key_overlap_fraction)
            or not 0.0 < key_overlap_fraction <= 1.0
            or not isinstance(selectivity, float)
            or not math.isfinite(selectivity)
            or not 0.0 < selectivity <= 1.0
        ):
            raise SpecError("edge key overlap or selectivity is invalid")
        if edge.get("type") not in {"one_to_many", "many_to_many"}:
            raise SpecError("unsupported edge type")
        if edge["type"] == "one_to_many":
            key_side_ndv = left_key_ndv if left_rows <= right_rows else right_key_ndv
            repeated_side_ndv = (
                right_key_ndv if left_rows <= right_rows else left_key_ndv
            )
            if key_side_ndv != minimum_rows:
                raise SpecError("one-to-many key endpoint must be unique")
            repeated_side_rows = max(left_rows, right_rows)
            if repeated_side_ndv >= repeated_side_rows:
                raise SpecError("one-to-many repeated endpoint must repeat keys")
        elif left_key_ndv >= left_rows or right_key_ndv >= right_rows:
            raise SpecError("many-to-many endpoints must both repeat keys")
        minimum_ndv = math.ceil(
            minimum_rows / ENDPOINT_MULTIPLICITY_SPREAD_MAX
        )
        if not (
            minimum_ndv <= left_key_ndv <= minimum_rows
            and minimum_ndv <= right_key_ndv <= minimum_rows
        ):
            raise SpecError("endpoint key NDV is outside the generator bounds")
        adjacency[left].append(right)
        adjacency[right].append(left)

    visited = {0}
    queue = deque([0])
    while queue:
        current = queue.popleft()
        for neighbor in adjacency[current]:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    if len(visited) != spec.n:
        raise SpecError("graph is disconnected")

    balance = graph.get("root_balance")
    if not isinstance(balance, Mapping):
        raise SpecError("root_balance must be an object")
    raw_average_growth = balance.get("raw_average_growth")
    edge_scale = balance.get("edge_scale")
    if (
        not isinstance(raw_average_growth, float)
        or not math.isfinite(raw_average_growth)
        or raw_average_growth <= 0.0
        or not isinstance(edge_scale, float)
        or not math.isfinite(edge_scale)
        or not 0.0 < edge_scale <= 1.0
    ):
        raise SpecError("root_balance values are invalid")

    # Root balance is recorded separately, while each edge stores the final
    # effective overlap. Dividing by the common scale recovers the sampled
    # overlap needed to validate the generator bounds and balance calculation.
    raw_overlaps = [
        edge["key_overlap_fraction"] / edge_scale for edge in edges
    ]
    for edge, raw_overlap in zip(edges, raw_overlaps):
        minimum_overlap = (
            ONE_TO_MANY_OVERLAP_MIN
            if edge["type"] == "one_to_many"
            else MANY_TO_MANY_OVERLAP_MIN
        )
        if not minimum_overlap <= raw_overlap <= 1.0 + 1e-15:
            raise SpecError("edge key overlap is outside the generator bounds")

    base_log_sum = math.fsum(math.log(node["base_rows"]) for node in nodes)
    typical_base_log = base_log_sum / spec.n
    raw_selectivities = [
        raw_overlap / max(edge["left_key_ndv"], edge["right_key_ndv"])
        for edge, raw_overlap in zip(edges, raw_overlaps)
    ]
    raw_root_log = base_log_sum + math.fsum(
        math.log(selectivity) for selectivity in raw_selectivities
    )
    raw_growth_log = (raw_root_log - typical_base_log) / (spec.n - 1)
    log_edge_scale = min(
        0.0, math.log(MAX_AVERAGE_JOIN_GROWTH) - raw_growth_log
    )
    expected_edge_scale = math.exp(log_edge_scale)
    if (
        not math.isclose(
            raw_average_growth,
            math.exp(raw_growth_log),
            rel_tol=1e-14,
            abs_tol=0.0,
        )
        or not math.isclose(
            edge_scale,
            expected_edge_scale,
            rel_tol=1e-14,
            abs_tol=0.0,
        )
    ):
        raise SpecError("root_balance does not match raw graph inputs")
    for edge in edges:
        if not math.isclose(
            edge["selectivity"],
            edge["key_overlap_fraction"]
            / max(edge["left_key_ndv"], edge["right_key_ndv"]),
            rel_tol=0.0,
            abs_tol=1e-18,
        ):
            raise SpecError("selectivity does not match key overlap and endpoint NDVs")
