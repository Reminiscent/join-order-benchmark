"""Build the deterministic graph IR used by the synthetic join-order workload.

The generator creates a uniformly labeled random tree, chooses an independent
uniform random anchor, and orients the tree only for cardinality generation.
Each directed edge receives a logical growth role. The complete query first
receives a random bounded root growth; contraction and expansion log budgets
are then distributed independently and unevenly to realize it without making
individual factors reciprocal pairs.

Growth is the logical authority. A fixed overlap distribution then realizes
that growth with repeated endpoint keys; integer NDVs and the final effective
overlap are derived without changing logical selectivity. No generator policy
is user configurable beyond relation count and the two reproducibility seeds.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP, localcontext
from typing import Any, Iterable, Mapping

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


GRAPH_FORMAT = "synthetic-join-order-graph"
BASE_ROWS_MIN = 1_000
BASE_ROWS_MAX = 1_000_000
PAYLOAD_WIDTH_MIN = 8
PAYLOAD_WIDTH_MAX = 256

# Workload policy. These values are intentionally code-owned, not configuration.
CARDINALITY_BUDGET = 100.0
MINIMUM_KEY_OVERLAP = 0.1

# Independent deterministic streams prevent unrelated generator changes from
# moving the anchor or another semantic dimension.
_BASE_ROWS_STREAM = 0x243F6A8885A308D3
_PAYLOAD_WIDTH_STREAM = 0xB7E151628AED2A6B
_ANCHOR_STREAM = 0x13198A2E03707344
_ROOT_GROWTH_STREAM = 0x9E3779B97F4A7C15
_EDGE_ROLE_STREAM = 0xA4093822299F31D0
_GROWTH_STREAM = 0x082EFA98EC4E6C89
_OVERLAP_STREAM = 0x452821E638D01377


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


def _stream(seed: int, salt: int) -> SplitMix64:
    return SplitMix64((seed ^ salt) & MASK64)


def _open_unit(rng: SplitMix64) -> float:
    """Return a deterministic floating-point sample strictly inside ``(0, 1)``."""
    return (rng.next_u64() + 1) / ((1 << 64) + 1)


def _allocate_log_budget(rng: SplitMix64, count: int) -> list[float]:
    """Split the query log budget into positive, deliberately uneven shares."""
    if count == 0:
        return []
    weights = [1.0 / _open_unit(rng) for _ in range(count)]
    total_weight = math.fsum(weights)
    budget = math.log(CARDINALITY_BUDGET)
    shares = [budget * weight / total_weight for weight in weights]
    largest = max(range(count), key=shares.__getitem__)
    shares[largest] += budget - math.fsum(shares)
    if any(not math.isfinite(share) or share <= 0.0 for share in shares):
        raise SpecError("log-budget allocation produced an invalid share")
    return shares


def _growth_assignments(
    cardinality_seed: int, edge_count: int
) -> tuple[list[tuple[str, float]], dict[str, int], float]:
    """Return deterministic roles and factors under the query-level budget."""
    maximum_log_budget = math.log(CARDINALITY_BUDGET)
    root_log_growth = maximum_log_budget * (
        2.0 * _open_unit(_stream(cardinality_seed, _ROOT_GROWTH_STREAM)) - 1.0
    )
    if edge_count == 1:
        role = "selective" if root_log_growth < 0.0 else "expanding"
        return [(role, math.exp(root_log_growth))], {
            "selective": int(role == "selective"),
            "neutral": 0,
            "expanding": int(role == "expanding"),
        }, root_log_growth

    role_rng = _stream(cardinality_seed, _EDGE_ROLE_STREAM)
    selective = [bool(role_rng.randbelow(2)) for _ in range(edge_count)]
    if all(selective):
        selective[role_rng.randbelow(edge_count)] = False
    elif not any(selective):
        selective[role_rng.randbelow(edge_count)] = True

    selective_count = sum(selective)
    expanding_count = edge_count - selective_count
    if root_log_growth < 0.0:
        contraction_budget = maximum_log_budget
        expansion_budget = maximum_log_budget + root_log_growth
    else:
        contraction_budget = maximum_log_budget - root_log_growth
        expansion_budget = maximum_log_budget
    growth_rng = _stream(cardinality_seed, _GROWTH_STREAM)
    contraction_shares = iter(
        share * contraction_budget / maximum_log_budget
        for share in _allocate_log_budget(growth_rng, selective_count)
    )
    expansion_shares = iter(
        share * expansion_budget / maximum_log_budget
        for share in _allocate_log_budget(growth_rng, expanding_count)
    )

    assignments = [
        (
            ("selective", math.exp(-next(contraction_shares)))
            if is_selective
            else ("expanding", math.exp(next(expansion_shares)))
        )
        for is_selective in selective
    ]
    return assignments, {
        "selective": selective_count,
        "neutral": 0,
        "expanding": expanding_count,
    }, root_log_growth


def canonical_json(value: Any) -> str:
    """Serialize canonical JSON; the terminating newline is part of the digest."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def graph_instance_id(graph: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(graph).encode("utf-8")).hexdigest()


def edge_id(left: int, right: int) -> str:
    left, right = sorted((left, right))
    return f"e_{left:04d}_{right:04d}"


def decode_prufer(sequence: Iterable[int], n: int) -> tuple[tuple[int, int], ...]:
    """Decode an ``n - 2`` item Prüfer sequence into one labeled tree."""
    sequence = tuple(sequence)
    if n < 2 or len(sequence) != n - 2:
        raise SpecError("a Prüfer sequence for n nodes must have length n - 2")
    if any(
        not isinstance(node, int)
        or isinstance(node, bool)
        or not 0 <= node < n
        for node in sequence
    ):
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
    edges.append(tuple(sorted((heapq.heappop(leaves), heapq.heappop(leaves)))))
    return tuple(sorted(edges))


def _log_uniform_int(rng: SplitMix64, low: int, high: int) -> int:
    if low == high:
        return low
    sample = rng.next_u64()
    with localcontext() as context:
        context.prec = 80
        fraction = Decimal(sample) / Decimal(1 << 64)
        value = (Decimal(low).ln() + fraction * (Decimal(high).ln() - Decimal(low).ln())).exp()
        rounded = int(value.to_integral_value(rounding=ROUND_HALF_UP))
    return min(high, max(low, rounded))


def _log_uniform_float(rng: SplitMix64, low: float, high: float) -> float:
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
        value = (low_decimal.ln() + fraction * (high_decimal.ln() - low_decimal.ln())).exp()
    result = float(value)
    if not math.isfinite(result) or not low <= result <= high:
        raise SpecError("log-uniform generation produced an invalid value")
    return result


def _floor_positive(value: float) -> int:
    with localcontext() as context:
        context.prec = 80
        return max(1, int(Decimal(str(value)).to_integral_value(rounding=ROUND_FLOOR)))


def edge_endpoint_key_ndv(edge: Mapping[str, Any], endpoint: int) -> int:
    if endpoint == edge["left"]:
        return edge["left_key_ndv"]
    if endpoint == edge["right"]:
        return edge["right_key_ndv"]
    raise SpecError("relation is not an endpoint of the edge")


def _orient_tree(
    edges: Iterable[tuple[int, int]], n: int, anchor: int
) -> dict[tuple[int, int], tuple[int, int]]:
    adjacency = [[] for _ in range(n)]
    for left, right in edges:
        adjacency[left].append(right)
        adjacency[right].append(left)
    parent = [-1] * n
    parent[anchor] = anchor
    queue = deque([anchor])
    while queue:
        current = queue.popleft()
        for neighbor in sorted(adjacency[current]):
            if parent[neighbor] == -1:
                parent[neighbor] = current
                queue.append(neighbor)
    return {
        tuple(sorted((node, parent[node]))): (parent[node], node)
        for node in range(n)
        if node != anchor
    }


def _connected_extrema(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    anchor: int,
) -> tuple[float, float]:
    """Return exact min/max connected-subset log cardinalities in O(n)."""
    children = [[] for _ in nodes]
    for edge in edges:
        children[edge["parent"]].append(edge)
    order = [anchor]
    for node in order:
        order.extend(edge["child"] for edge in children[node])
    minimum = [0.0] * len(nodes)
    maximum = [0.0] * len(nodes)
    for node in reversed(order):
        minimum[node] = maximum[node] = math.log(nodes[node]["base_rows"])
        for edge in children[node]:
            term = math.log(edge["selectivity"])
            child = edge["child"]
            minimum[node] += min(0.0, term + minimum[child])
            maximum[node] += max(0.0, term + maximum[child])
    return min(minimum), max(maximum)


def generate_graph(spec: GenerationSpec) -> dict[str, Any]:
    """Expand one minimal spec into the canonical bounded-growth graph IR."""
    topology_rng = SplitMix64(spec.graph_seed)
    sequence = tuple(topology_rng.randbelow(spec.n) for _ in range(spec.n - 2))
    bare_edges = decode_prufer(sequence, spec.n)

    base_rng = _stream(spec.cardinality_seed, _BASE_ROWS_STREAM)
    base_rows = [_log_uniform_int(base_rng, BASE_ROWS_MIN, BASE_ROWS_MAX) for _ in range(spec.n)]
    width_rng = _stream(spec.cardinality_seed, _PAYLOAD_WIDTH_STREAM)
    payload_widths = [
        _log_uniform_int(width_rng, PAYLOAD_WIDTH_MIN, PAYLOAD_WIDTH_MAX)
        for _ in range(spec.n)
    ]
    nodes = [
        {"id": node_id, "base_rows": rows, "payload_width": payload_widths[node_id]}
        for node_id, rows in enumerate(base_rows)
    ]

    anchor = _stream(spec.cardinality_seed, _ANCHOR_STREAM).randbelow(spec.n)
    directions = _orient_tree(bare_edges, spec.n, anchor)

    edge_count = spec.n - 1
    growth_assignments, role_counts, root_log_growth = _growth_assignments(
        spec.cardinality_seed, edge_count
    )

    overlap_rng = _stream(spec.cardinality_seed, _OVERLAP_STREAM)
    edges: list[dict[str, Any]] = []
    for index, (left, right) in enumerate(bare_edges):
        parent, child = directions[(left, right)]
        role, growth = growth_assignments[index]

        overlap_upper = min(1.0, growth)
        target_overlap = _log_uniform_float(
            overlap_rng, min(MINIMUM_KEY_OVERLAP, overlap_upper), overlap_upper
        )
        child_rows = base_rows[child]
        child_key_ndv = min(
            child_rows - 1,
            _floor_positive(child_rows * target_overlap / growth),
        )
        parent_key_ndv = min(base_rows[parent] - 1, child_key_ndv)
        selectivity = growth / child_rows
        effective_overlap = selectivity * child_key_ndv
        endpoint_ndvs = {parent: parent_key_ndv, child: child_key_ndv}
        edges.append({
            "id": edge_id(left, right),
            "left": left,
            "right": right,
            "parent": parent,
            "child": child,
            "growth_role": role,
            "growth_factor": growth,
            "left_key_ndv": endpoint_ndvs[left],
            "right_key_ndv": endpoint_ndvs[right],
            "key_overlap_fraction": effective_overlap,
            "selectivity": selectivity,
        })

    minimum_log, maximum_log = _connected_extrema(nodes, edges, anchor)
    root_log = math.fsum(math.log(node["base_rows"]) for node in nodes) + math.fsum(
        math.log(edge["selectivity"]) for edge in edges
    )
    graph = {
        "format": GRAPH_FORMAT,
        "source_spec": spec.to_dict(),
        "nodes": nodes,
        "edges": edges,
        "cardinality_model": {
            "anchor_relation": anchor,
            "budget": CARDINALITY_BUDGET,
            "selective_edge_count": role_counts["selective"],
            "expanding_edge_count": role_counts["expanding"],
            "neutral_edge_count": role_counts["neutral"],
            "root_growth_factor": math.exp(root_log_growth),
            "contraction_log_budget": math.fsum(
                -math.log(growth)
                for role, growth in growth_assignments
                if role == "selective"
            ),
            "expansion_log_budget": math.fsum(
                math.log(growth)
                for role, growth in growth_assignments
                if role == "expanding"
            ),
            "root_log_rows": root_log,
            "minimum_connected_log_rows": minimum_log,
            "maximum_connected_log_rows": maximum_log,
        },
    }
    validate_graph(graph)
    return graph


def validate_graph(graph: Mapping[str, Any]) -> None:
    """Reject malformed graphs and drift in every derived workload invariant."""
    if graph.get("format") != GRAPH_FORMAT:
        raise SpecError("unsupported graph format")
    raw_spec = graph.get("source_spec")
    if not isinstance(raw_spec, Mapping):
        raise SpecError("source_spec must be an object")
    spec = GenerationSpec.from_dict(raw_spec)
    nodes = graph.get("nodes")
    edges = graph.get("edges")
    model = graph.get("cardinality_model")
    if (
        not isinstance(nodes, list)
        or not isinstance(edges, list)
        or not isinstance(model, Mapping)
    ):
        raise SpecError("nodes, edges, and cardinality_model have invalid types")
    if [node.get("id") for node in nodes] != list(range(spec.n)):
        raise SpecError("nodes must be ordered by contiguous canonical id")
    if any(
        not isinstance(node.get("base_rows"), int)
        or not BASE_ROWS_MIN <= node["base_rows"] <= BASE_ROWS_MAX
        for node in nodes
    ):
        raise SpecError("node base_rows fall outside generator bounds")
    if any(
        not isinstance(node.get("payload_width"), int)
        or not PAYLOAD_WIDTH_MIN <= node["payload_width"] <= PAYLOAD_WIDTH_MAX
        for node in nodes
    ):
        raise SpecError("node payload_width falls outside generator bounds")
    if len(edges) != spec.n - 1:
        raise SpecError("tree must have exactly n - 1 edges")

    anchor = model.get("anchor_relation")
    if not isinstance(anchor, int) or isinstance(anchor, bool) or not 0 <= anchor < spec.n:
        raise SpecError("cardinality anchor is invalid")
    expected_anchor = _stream(spec.cardinality_seed, _ANCHOR_STREAM).randbelow(spec.n)
    if anchor != expected_anchor:
        raise SpecError("cardinality anchor does not match the fixed random policy")
    expected_assignments, expected_counts, expected_root_log_growth = _growth_assignments(
        spec.cardinality_seed, len(edges)
    )
    if model.get("budget") != CARDINALITY_BUDGET or any(
        model.get(f"{role}_edge_count") != count for role, count in expected_counts.items()
    ):
        raise SpecError("cardinality model policy does not match generator constants")
    expected_budgets = {
        "contraction_log_budget": math.fsum(
            -math.log(growth)
            for role, growth in expected_assignments
            if role == "selective"
        ),
        "expansion_log_budget": math.fsum(
            math.log(growth)
            for role, growth in expected_assignments
            if role == "expanding"
        ),
    }
    for name, expected_budget in expected_budgets.items():
        value = model.get(name)
        if (
            not isinstance(value, float)
            or not math.isfinite(value)
            or not math.isclose(value, expected_budget, rel_tol=0.0, abs_tol=1e-14)
        ):
            raise SpecError(f"{name} does not match the cardinality budget")
    root_growth_factor = model.get("root_growth_factor")
    if (
        not isinstance(root_growth_factor, float)
        or not math.isfinite(root_growth_factor)
        or not math.isclose(
            root_growth_factor,
            math.exp(expected_root_log_growth),
            rel_tol=0.0,
            abs_tol=1e-15,
        )
    ):
        raise SpecError("root growth does not match the seeded policy")

    seen: set[tuple[int, int]] = set()
    adjacency = [[] for _ in nodes]
    roles = {role: 0 for role in expected_counts}
    growth_logs: list[float] = []
    previous_id = ""
    for edge in edges:
        left, right = edge.get("left"), edge.get("right")
        parent, child = edge.get("parent"), edge.get("child")
        if (
            not isinstance(left, int)
            or not isinstance(right, int)
            or not 0 <= left < right < spec.n
        ):
            raise SpecError("edge endpoints must be canonical and in range")
        if {parent, child} != {left, right}:
            raise SpecError("edge generation direction does not match endpoints")
        pair = (left, right)
        expected_id = edge_id(left, right)
        if pair in seen or edge.get("id") != expected_id or expected_id <= previous_id:
            raise SpecError("edges must be unique, canonical, and sorted")
        seen.add(pair)
        previous_id = expected_id
        adjacency[left].append(right)
        adjacency[right].append(left)

        role = edge.get("growth_role")
        growth = edge.get("growth_factor")
        selectivity = edge.get("selectivity")
        overlap = edge.get("key_overlap_fraction")
        if (
            role not in roles
            or not isinstance(growth, float)
            or not math.isfinite(growth)
            or growth <= 0
        ):
            raise SpecError("edge growth role or factor is invalid")
        roles[role] += 1
        expected_role, expected_growth = expected_assignments[len(growth_logs)]
        if role != expected_role or not math.isclose(
            growth, expected_growth, rel_tol=0.0, abs_tol=1e-15
        ):
            raise SpecError("edge growth does not match the seeded budget allocation")
        if (
            (role == "selective" and not growth < 1.0)
            or (role == "neutral" and growth != 1.0)
            or (role == "expanding" and not growth > 1.0)
        ):
            raise SpecError("edge growth factor does not match its role")
        if not isinstance(selectivity, float) or not math.isfinite(selectivity):
            raise SpecError("edge selectivity is invalid")
        child_rows = nodes[child]["base_rows"]
        if not math.isclose(
            selectivity, growth / child_rows, rel_tol=0.0, abs_tol=1e-18
        ):
            raise SpecError("selectivity does not match growth and child rows")
        left_ndv, right_ndv = edge.get("left_key_ndv"), edge.get("right_key_ndv")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in (left_ndv, right_ndv)
        ):
            raise SpecError("endpoint key NDVs must be positive integers")
        if (
            left_ndv >= nodes[left]["base_rows"]
            or right_ndv >= nodes[right]["base_rows"]
        ):
            raise SpecError("overlapping endpoint keys must repeat")
        child_ndv = left_ndv if child == left else right_ndv
        parent_ndv = right_ndv if child == left else left_ndv
        if parent_ndv > child_ndv:
            raise SpecError("parent key NDV must not exceed child key NDV")
        if (
            not isinstance(overlap, float)
            or not math.isfinite(overlap)
            or overlap <= 0.0
            or overlap > min(1.0, growth)
            or overlap + selectivity
            < min(MINIMUM_KEY_OVERLAP, growth) - 1e-15
        ):
            raise SpecError("edge key overlap is outside generator bounds")
        if not math.isclose(
            selectivity,
            overlap / max(left_ndv, right_ndv),
            rel_tol=1e-15,
            abs_tol=1e-18,
        ):
            raise SpecError("selectivity does not match key overlap and endpoint NDVs")
        growth_logs.append(math.log(growth))

    if roles != expected_counts:
        raise SpecError("edge role counts do not match cardinality model")
    visited = {anchor}
    queue = deque([anchor])
    while queue:
        current = queue.popleft()
        for neighbor in adjacency[current]:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    if len(visited) != spec.n:
        raise SpecError("graph is disconnected")

    directions = _orient_tree(tuple(seen), spec.n, anchor)
    if any(
        directions[(edge["left"], edge["right"])]
        != (edge["parent"], edge["child"])
        for edge in edges
    ):
        raise SpecError("edge directions do not follow the recorded anchor")
    if not math.isclose(
        math.fsum(growth_logs),
        expected_root_log_growth,
        rel_tol=0.0,
        abs_tol=1e-13,
    ):
        raise SpecError("full-query growth does not match the query policy")

    minimum_log, maximum_log = _connected_extrema(nodes, edges, anchor)
    root_log = math.fsum(
        math.log(node["base_rows"]) for node in nodes
    ) + math.fsum(math.log(edge["selectivity"]) for edge in edges)
    expected_values = {
        "root_log_rows": root_log,
        "minimum_connected_log_rows": minimum_log,
        "maximum_connected_log_rows": maximum_log,
    }
    if any(
        not isinstance(model.get(name), float)
        or not math.isfinite(model[name])
        or not math.isclose(model[name], value, rel_tol=1e-14, abs_tol=1e-14)
        for name, value in expected_values.items()
    ):
        raise SpecError("cardinality model extrema do not match graph inputs")
    if not math.isclose(
        root_log,
        math.log(nodes[anchor]["base_rows"]) + expected_root_log_growth,
        rel_tol=0.0,
        abs_tol=1e-11,
    ):
        raise SpecError("full-query rows do not match anchor and root growth")
    if minimum_log < math.log(BASE_ROWS_MIN / CARDINALITY_BUDGET) - 1e-11:
        raise SpecError("minimum connected cardinality exceeds the budget")
    if maximum_log > math.log(BASE_ROWS_MAX * CARDINALITY_BUDGET) + 1e-11:
        raise SpecError("maximum connected cardinality exceeds the budget")
