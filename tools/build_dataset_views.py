#!/usr/bin/env python3

import csv
import json
import math
from collections import defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "results"
OUT_ROOT = REPO_ROOT / "analysis" / "by_dataset"


def pct(values, p):
    if not values:
        return ""
    vals = sorted(values)
    if len(vals) == 1:
        return f"{vals[0]:.3f}"
    k = (len(vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return f"{vals[int(k)]:.3f}"
    v = vals[f] * (c - k) + vals[c] * (k - f)
    return f"{v:.3f}"


def gmean(values):
    vals = [v for v in values if v > 0]
    if not vals:
        return ""
    return f"{math.exp(sum(math.log(v) for v in vals) / len(vals)):.3f}"


def float_or_none(v):
    if v in ("", None):
        return None
    return float(v)


def int_or_none(v):
    if v in ("", None):
        return None
    return int(v)


def run_sort_key(run_cfg):
    return (run_cfg.get("timestamp") or "", run_cfg["run_id"])


def scan_runs():
    runs = {}
    if not RESULTS_DIR.is_dir():
        return runs
    for path in sorted(RESULTS_DIR.iterdir()):
        if not path.is_dir():
            continue
        run_json = path / "run.json"
        if not run_json.is_file():
            continue
        cfg = json.loads(run_json.read_text())
        cfg["path"] = str(path)
        cfg["summary_path"] = str(path / "summary.csv")
        cfg["has_summary"] = (path / "summary.csv").is_file()
        runs[cfg["run_id"]] = cfg
    return runs


def is_canonical_eligible(run_cfg):
    return run_cfg.get("stabilize") != "none"


def load_summary_rows(run_cfg):
    summary_path = Path(run_cfg["summary_path"])
    if not summary_path.is_file():
        return []
    rows = []
    with summary_path.open(newline="") as f:
        for row in csv.DictReader(f):
            row2 = dict(row)
            row2["source_run_id"] = run_cfg["run_id"]
            row2["source_timestamp"] = run_cfg.get("timestamp", "")
            row2["source_db"] = run_cfg.get("db", "")
            row2["source_min_join"] = run_cfg.get("min_join")
            row2["source_max_join"] = run_cfg.get("max_join")
            row2["source_query_offset"] = run_cfg.get("query_offset", 0)
            row2["source_max_queries"] = run_cfg.get("max_queries")
            row2["source_repetitions"] = run_cfg.get("repetitions")
            row2["source_statement_timeout_ms"] = run_cfg.get("statement_timeout_ms")
            row2["source_stabilize"] = run_cfg.get("stabilize", "")
            row2["ok_reps"] = int(row2["ok_reps"])
            row2["err_reps"] = int(row2["err_reps"])
            row2["planning_ms_min"] = float_or_none(row2["planning_ms_min"])
            row2["total_ms_min"] = float_or_none(row2["total_ms_min"])
            row2["execution_ms_min"] = float_or_none(row2["execution_ms_min"])
            row2["join_size"] = int_or_none(row2["join_size"])
            rows.append(row2)
    return rows


def as_str(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.3f}"
    return str(v)


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        w.writeheader()
        for row in rows:
            w.writerow({k: as_str(row.get(k)) for k in fieldnames})


def build_dataset_view(dataset, run_cfgs, selected_rows):
    out_dir = OUT_ROOT / dataset
    out_dir.mkdir(parents=True, exist_ok=True)

    rows_by_run = defaultdict(list)
    for row in selected_rows:
        rows_by_run[row["source_run_id"]].append(row)

    canonical_rows = sorted(
        selected_rows,
        key=lambda r: (r["query_id"], r["algo"]),
    )
    canonical_fieldnames = [
        "dataset",
        "algo",
        "query_id",
        "query_label",
        "query_path",
        "join_size",
        "planning_ms_min",
        "total_ms_min",
        "execution_ms_min",
        "ok_reps",
        "err_reps",
        "explain_analyze_status",
        "explain_analyze_artifact",
        "source_run_id",
        "source_timestamp",
        "source_min_join",
        "source_max_join",
        "source_query_offset",
        "source_max_queries",
        "source_statement_timeout_ms",
        "source_stabilize",
    ]
    write_csv(out_dir / "canonical_summary.csv", canonical_fieldnames, canonical_rows)

    distinct_queries = sorted({r["query_id"] for r in canonical_rows})
    join_sizes = sorted(r["join_size"] for r in canonical_rows if r["join_size"] is not None)

    summary_rows = []
    algos = sorted({r["algo"] for r in canonical_rows})
    for algo in algos:
        algo_rows = [r for r in canonical_rows if r["algo"] == algo]
        ok_rows = [r for r in algo_rows if r["ok_reps"] > 0 and r["total_ms_min"] is not None]
        planning_vals = [r["planning_ms_min"] for r in ok_rows if r["planning_ms_min"] is not None]
        total_vals = [r["total_ms_min"] for r in ok_rows if r["total_ms_min"] is not None]
        exec_vals = [r["execution_ms_min"] for r in ok_rows if r["execution_ms_min"] is not None]
        summary_rows.append(
            {
                "dataset": dataset,
                "algo": algo,
                "queries_total": len(algo_rows),
                "queries_ok": len(ok_rows),
                "queries_err": sum(1 for r in algo_rows if r["err_reps"] > 0 and r["ok_reps"] == 0),
                "queries_partial_err": sum(1 for r in algo_rows if r["err_reps"] > 0 and r["ok_reps"] > 0),
                "selected_run_count": len({r["source_run_id"] for r in algo_rows}),
                "explain_analyze_ok": sum(1 for r in algo_rows if r.get("explain_analyze_status") == "ok"),
                "explain_analyze_error": sum(1 for r in algo_rows if r.get("explain_analyze_status") == "error"),
                "planning_gmean_ms": gmean(planning_vals),
                "planning_p50_ms": pct(planning_vals, 0.50),
                "planning_p90_ms": pct(planning_vals, 0.90),
                "planning_p95_ms": pct(planning_vals, 0.95),
                "planning_p99_ms": pct(planning_vals, 0.99),
                "planning_max_ms": f"{max(planning_vals):.3f}" if planning_vals else "",
                "total_gmean_ms": gmean(total_vals),
                "total_p50_ms": pct(total_vals, 0.50),
                "total_p90_ms": pct(total_vals, 0.90),
                "total_p95_ms": pct(total_vals, 0.95),
                "total_p99_ms": pct(total_vals, 0.99),
                "total_max_ms": f"{max(total_vals):.3f}" if total_vals else "",
                "execution_gmean_ms": gmean(exec_vals),
                "execution_p50_ms": pct(exec_vals, 0.50),
                "execution_p90_ms": pct(exec_vals, 0.90),
                "execution_p95_ms": pct(exec_vals, 0.95),
                "execution_p99_ms": pct(exec_vals, 0.99),
                "execution_max_ms": f"{max(exec_vals):.3f}" if exec_vals else "",
            }
        )
    write_csv(
        out_dir / "summary_by_algo.csv",
        [
            "dataset",
            "algo",
            "queries_total",
            "queries_ok",
            "queries_err",
            "queries_partial_err",
            "selected_run_count",
            "explain_analyze_ok",
            "explain_analyze_error",
            "planning_gmean_ms",
            "planning_p50_ms",
            "planning_p90_ms",
            "planning_p95_ms",
            "planning_p99_ms",
            "planning_max_ms",
            "total_gmean_ms",
            "total_p50_ms",
            "total_p90_ms",
            "total_p95_ms",
            "total_p99_ms",
            "total_max_ms",
            "execution_gmean_ms",
            "execution_p50_ms",
            "execution_p90_ms",
            "execution_p95_ms",
            "execution_p99_ms",
            "execution_max_ms",
        ],
        summary_rows,
    )

    geqo_rows = {r["query_id"]: r for r in canonical_rows if r["algo"] == "geqo"}
    tail_rows = []
    for algo in algos:
        if algo == "geqo":
            continue
        ratios = []
        pair_count = 0
        for row in canonical_rows:
            if row["algo"] != algo:
                continue
            geqo = geqo_rows.get(row["query_id"])
            if geqo is None:
                continue
            if row["total_ms_min"] is None or geqo["total_ms_min"] is None:
                continue
            if geqo["total_ms_min"] < 1.0:
                continue
            pair_count += 1
            ratios.append(row["total_ms_min"] / geqo["total_ms_min"])
        tail_rows.append(
            {
                "dataset": dataset,
                "algo": algo,
                "pair_count": pair_count,
                "ratio_gmean": gmean(ratios),
                "ratio_p50": pct(ratios, 0.50),
                "ratio_p90": pct(ratios, 0.90),
                "ratio_p95": pct(ratios, 0.95),
                "ratio_p99": pct(ratios, 0.99),
                "ratio_max": f"{max(ratios):.3f}" if ratios else "",
                "count_ge_2x": sum(1 for v in ratios if v >= 2.0),
                "count_ge_5x": sum(1 for v in ratios if v >= 5.0),
                "count_ge_10x": sum(1 for v in ratios if v >= 10.0),
            }
        )
    write_csv(
        out_dir / "tail_vs_geqo.csv",
        [
            "dataset",
            "algo",
            "pair_count",
            "ratio_gmean",
            "ratio_p50",
            "ratio_p90",
            "ratio_p95",
            "ratio_p99",
            "ratio_max",
            "count_ge_2x",
            "count_ge_5x",
            "count_ge_10x",
        ],
        tail_rows,
    )

    join_rows = []
    by_join_algo = defaultdict(list)
    for row in canonical_rows:
        by_join_algo[(row["join_size"], row["algo"])].append(row)
    for (join_size, algo), rows in sorted(by_join_algo.items()):
        join_rows.append(
            {
                "dataset": dataset,
                "join_size": join_size,
                "algo": algo,
                "queries_total": len(rows),
                "queries_ok": sum(1 for r in rows if r["ok_reps"] > 0 and r["total_ms_min"] is not None),
                "queries_err": sum(1 for r in rows if r["err_reps"] > 0 and r["ok_reps"] == 0),
                "source_runs": ";".join(sorted({r["source_run_id"] for r in rows})),
            }
        )
    write_csv(
        out_dir / "coverage_by_join_size.csv",
        ["dataset", "join_size", "algo", "queries_total", "queries_ok", "queries_err", "source_runs"],
        join_rows,
    )

    source_rows = []
    for run in sorted(run_cfgs, key=run_sort_key):
        summary_rows_for_run = load_summary_rows(run) if run["has_summary"] else []
        source_rows.append(
            {
                "run_id": run["run_id"],
                "timestamp": run.get("timestamp", ""),
                "dataset": dataset,
                "db": run.get("db", ""),
                "eligible_for_canonical": 1 if is_canonical_eligible(run) else 0,
                "has_summary": 1 if run["has_summary"] else 0,
                "capture_explain_analyze_json": 1 if run.get("capture_explain_analyze_json") else 0,
                "min_join": run.get("min_join"),
                "max_join": run.get("max_join"),
                "query_offset": run.get("query_offset", 0),
                "max_queries": run.get("max_queries"),
                "repetitions": run.get("repetitions"),
                "statement_timeout_ms": run.get("statement_timeout_ms"),
                "stabilize": run.get("stabilize", ""),
                "algo_names": ";".join(a["name"] for a in run.get("algos", [])),
                "summary_rows": len(summary_rows_for_run),
                "selected_rows": len(rows_by_run.get(run["run_id"], [])),
            }
        )
    write_csv(
        out_dir / "source_runs.csv",
        [
            "run_id",
            "timestamp",
            "dataset",
            "db",
            "eligible_for_canonical",
            "has_summary",
            "capture_explain_analyze_json",
            "min_join",
            "max_join",
            "query_offset",
            "max_queries",
            "repetitions",
            "statement_timeout_ms",
            "stabilize",
            "algo_names",
            "summary_rows",
            "selected_rows",
        ],
        source_rows,
    )

    return {
        "dataset": dataset,
        "queries": len(distinct_queries),
        "selected_rows": len(canonical_rows),
        "selected_runs": len({r["source_run_id"] for r in canonical_rows}),
        "source_runs": len(run_cfgs),
        "join_size_min": join_sizes[0] if join_sizes else "",
        "join_size_p50": pct(join_sizes, 0.50),
        "join_size_p90": pct(join_sizes, 0.90),
        "join_size_max": join_sizes[-1] if join_sizes else "",
        "canonical_summary": str((out_dir / "canonical_summary.csv").relative_to(REPO_ROOT)),
        "summary_by_algo": str((out_dir / "summary_by_algo.csv").relative_to(REPO_ROOT)),
        "tail_vs_geqo": str((out_dir / "tail_vs_geqo.csv").relative_to(REPO_ROOT)),
        "coverage_by_join_size": str((out_dir / "coverage_by_join_size.csv").relative_to(REPO_ROOT)),
        "source_runs_csv": str((out_dir / "source_runs.csv").relative_to(REPO_ROOT)),
    }


def main():
    runs = scan_runs()
    by_dataset_runs = defaultdict(list)
    latest_rows = {}

    for run in runs.values():
        dataset = run.get("dataset")
        if not dataset:
            continue
        by_dataset_runs[dataset].append(run)
        if not run["has_summary"] or not is_canonical_eligible(run):
            continue
        for row in load_summary_rows(run):
            key = (row["dataset"], row["query_id"], row["algo"])
            current = latest_rows.get(key)
            if current is None:
                latest_rows[key] = row
                continue
            old_run = runs[current["source_run_id"]]
            if run_sort_key(run) >= run_sort_key(old_run):
                latest_rows[key] = row

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    readme = OUT_ROOT / "README.md"
    readme.write_text(
        "# Dataset-Organized Benchmark Views\n\n"
        "These files provide a canonical view over `results/<run_id>/summary.csv` organized by dataset rather than run date.\n\n"
        "Selection policy:\n\n"
        "- ignore smoke-style runs with `stabilize=none`\n"
        "- group by `(dataset, query_id, algo)`\n"
        "- keep the latest run attempt for that key (later runs supersede earlier ones)\n"
        "- compute dataset-level summaries from those canonical rows\n"
        "- keep raw dated run directories on disk for traceability, but ignore superseded rows in the canonical view\n",
    )

    index_rows = []
    for dataset in sorted(by_dataset_runs):
        selected = [row for key, row in latest_rows.items() if key[0] == dataset]
        index_rows.append(build_dataset_view(dataset, by_dataset_runs[dataset], selected))

    write_csv(
        OUT_ROOT / "index.csv",
        [
            "dataset",
            "queries",
            "selected_rows",
            "selected_runs",
            "source_runs",
            "join_size_min",
            "join_size_p50",
            "join_size_p90",
            "join_size_max",
            "canonical_summary",
            "summary_by_algo",
            "tail_vs_geqo",
            "coverage_by_join_size",
            "source_runs_csv",
        ],
        index_rows,
    )


if __name__ == "__main__":
    main()
