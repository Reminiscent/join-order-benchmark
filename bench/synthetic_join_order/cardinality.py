"""Compute construction-independent cardinalities from the expanded graph.

For a relation subset ``S``, this module multiplies the base rows in ``S`` by
the selectivity of every graph edge fully contained in ``S``. Computing from
the complete canonical subset makes the result independent of the join order
that first constructed it.

Start with ``subset_cardinality``; it implements the workload formula for one
relation subset. ``subset_log_cardinality`` performs the same calculation in
log space to avoid overflow. The connectivity and recurrence helpers support
exhaustive checks on small golden graphs.
"""

from __future__ import annotations

import itertools
import math
from collections import deque
from typing import Any, Iterable, Mapping

from .graph import validate_graph


MAXIMUM_ROWCOUNT = 1.0e100
LOG_MAXIMUM_ROWCOUNT = math.log(MAXIMUM_ROWCOUNT)


class CardinalityError(ValueError):
    """Raised when a relation subset cannot be assigned a cardinality."""


def canonical_relids(relids: Iterable[int], n: int) -> tuple[int, ...]:
    """Validate and sort one nonempty relation subset."""
    raw = tuple(relids)
    if not raw:
        raise CardinalityError("subset must not be empty")
    if any(not isinstance(relid, int) or isinstance(relid, bool) for relid in raw):
        raise CardinalityError("canonical relation ids must be integers")
    if len(set(raw)) != len(raw):
        raise CardinalityError("canonical relation ids must not contain duplicates")
    result = tuple(sorted(raw))
    if result[0] < 0 or result[-1] >= n:
        raise CardinalityError("canonical relation id is out of range")
    return result


def _adjacency(graph: Mapping[str, Any]) -> list[list[int]]:
    adjacency = [[] for _ in graph["nodes"]]
    for edge in graph["edges"]:
        adjacency[edge["left"]].append(edge["right"])
        adjacency[edge["right"]].append(edge["left"])
    return adjacency


def is_connected_subset(graph: Mapping[str, Any], relids: Iterable[int]) -> bool:
    """Return whether the induced subgraph over ``relids`` is connected."""
    subset = canonical_relids(relids, len(graph["nodes"]))
    wanted = set(subset)
    visited = {subset[0]}
    queue = deque([subset[0]])
    adjacency = _adjacency(graph)
    while queue:
        current = queue.popleft()
        for neighbor in adjacency[current]:
            if neighbor in wanted and neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    return visited == wanted


def subset_log_cardinality(graph: Mapping[str, Any], relids: Iterable[int]) -> float:
    """Return the unclamped log cardinality of one relation subset.

    Log space turns products into sums and avoids overflow while large
    relation sets are being evaluated.
    """
    validate_graph(graph)
    subset = canonical_relids(relids, len(graph["nodes"]))
    wanted = set(subset)
    value = math.fsum(math.log(graph["nodes"][relid]["base_rows"]) for relid in subset)
    edge_terms = [
        math.log(edge["selectivity"])
        for edge in graph["edges"]
        if edge["left"] in wanted and edge["right"] in wanted
    ]
    result = value + math.fsum(edge_terms)
    if not math.isfinite(result):
        raise CardinalityError("subset formula produced a non-finite log cardinality")
    return result


def clamp_row_est_from_log(log_cardinality: float) -> float:
    """Mirror PostgreSQL ``clamp_row_est`` without overflowing ``exp``."""
    if not math.isfinite(log_cardinality):
        raise CardinalityError("log cardinality must be finite")
    if log_cardinality <= 0.0:
        return 1.0
    if log_cardinality >= LOG_MAXIMUM_ROWCOUNT:
        return MAXIMUM_ROWCOUNT
    return float(round(math.exp(log_cardinality)))


def subset_cardinality(graph: Mapping[str, Any], relids: Iterable[int]) -> float:
    """Return one subset cardinality with PostgreSQL-compatible clamping."""
    return clamp_row_est_from_log(subset_log_cardinality(graph, relids))


def crossing_edges(
    graph: Mapping[str, Any], left_relids: Iterable[int], right_relids: Iterable[int]
) -> tuple[Mapping[str, Any], ...]:
    """Return graph edges whose endpoints lie on opposite join inputs."""
    left = set(canonical_relids(left_relids, len(graph["nodes"])))
    right = set(canonical_relids(right_relids, len(graph["nodes"])))
    if left & right:
        raise CardinalityError("join inputs must be disjoint")
    return tuple(
        edge
        for edge in graph["edges"]
        if (edge["left"] in left and edge["right"] in right)
        or (edge["right"] in left and edge["left"] in right)
    )


def recurrence_log_cardinality(
    graph: Mapping[str, Any], left_relids: Iterable[int], right_relids: Iterable[int]
) -> float:
    """Rebuild a connected tree subset from two connected inputs.

    A tree has exactly one crossing edge between such inputs. This recurrence
    is used to prove that split-based construction matches the direct subset
    formula.
    """
    left = canonical_relids(left_relids, len(graph["nodes"]))
    right = canonical_relids(right_relids, len(graph["nodes"]))
    if not is_connected_subset(graph, left) or not is_connected_subset(graph, right):
        raise CardinalityError("join inputs must be connected")
    crossing = crossing_edges(graph, left, right)
    if len(crossing) != 1:
        raise CardinalityError("tree join inputs must have exactly one crossing edge")
    return (
        subset_log_cardinality(graph, left)
        + subset_log_cardinality(graph, right)
        + math.log(crossing[0]["selectivity"])
    )


def connected_subsets(graph: Mapping[str, Any]) -> tuple[tuple[int, ...], ...]:
    """Enumerate connected subsets for small qualification graphs only."""
    n = len(graph["nodes"])
    if n > 20:
        raise CardinalityError("connected subset enumeration is limited to n <= 20")
    result = []
    for size in range(1, n + 1):
        for subset in itertools.combinations(range(n), size):
            if is_connected_subset(graph, subset):
                result.append(subset)
    return tuple(result)
