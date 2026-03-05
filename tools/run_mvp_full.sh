#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

# Prefer binaries from the custom PostgreSQL build when provided.
if [[ -n "${PG_BUILD_DIR:-}" && -d "$PG_BUILD_DIR/bin" ]]; then
  export PATH="$PG_BUILD_DIR/bin:$PATH"
fi

HOST=${PG_BENCH_HOST:-localhost}
PORT=${PG_BENCH_PORT:-54321}
CONN_ARGS=(--host "$HOST" --port "$PORT")
if [[ -n "${PG_BENCH_USER:-}" ]]; then
  CONN_ARGS+=(--user "$PG_BENCH_USER")
fi

REPS=${BENCH_REPS:-3}
STABILIZE=${BENCH_STABILIZE:-analyze}
DP_MAX_JOIN=${DP_MAX_JOIN:-20}
INCLUDE_IMDB_CEB_3K=${INCLUDE_IMDB_CEB_3K:-0}
STATEMENT_TIMEOUT_MS=${STATEMENT_TIMEOUT_MS:-0}
ONLY_DATASETS=${ONLY_DATASETS:-}
SKIP_DATASETS=${SKIP_DATASETS:-}

IMDB_DB=${IMDB_DB:-imdb_mvp}
SQLITE_DB=${SQLITE_DB:-select5_mvp}
GPUQO_CHAIN_DB=${GPUQO_CHAIN_DB:-gpuqo_chain_mvp}
GPUQO_CLIQUE_DB=${GPUQO_CLIQUE_DB:-gpuqo_clique_mvp}
GPUQO_STAR_DB=${GPUQO_STAR_DB:-gpuqo_star_mvp}
GPUQO_SNOWFLAKE_DB=${GPUQO_SNOWFLAKE_DB:-gpuqo_snowflake_mvp}

# 6 algorithms (full set).
ALGOS_FULL=(
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off"
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off"
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost"
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size"
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity"
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
)

# Non-DP algorithms for very wide joins.
ALGOS_NO_DP=(
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off"
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost"
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size"
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity"
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
)

COMMON_RUN_ARGS=(
  "${CONN_ARGS[@]}"
  --reps "$REPS"
  --stabilize "$STABILIZE"
)
if [[ "$STATEMENT_TIMEOUT_MS" -gt 0 ]]; then
  COMMON_RUN_ARGS+=(--statement-timeout-ms "$STATEMENT_TIMEOUT_MS")
fi

normalize_dataset_list() {
  local raw=$1
  raw=${raw//,/ }
  raw=${raw//:/ }
  # shellcheck disable=SC2206
  local parts=($raw)
  printf '%s\n' "${parts[@]}"
}

is_valid_dataset() {
  local dataset=$1
  case "$dataset" in
    job|job_complex|imdb_ceb_3k|gpuqo_chain_small|gpuqo_clique_small|gpuqo_star_small|sqlite_select5|gpuqo_snowflake_small)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

contains_dataset() {
  local needle=$1
  shift || true
  local item
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

DEFAULT_DATASETS=(
  job
  job_complex
  gpuqo_chain_small
  gpuqo_clique_small
  gpuqo_star_small
  sqlite_select5
  gpuqo_snowflake_small
)

SELECTED_DATASETS=()
if [[ -n "$ONLY_DATASETS" ]]; then
  while IFS= read -r ds; do
    [[ -z "$ds" ]] && continue
    if ! is_valid_dataset "$ds"; then
      echo "[mvp-full] unknown dataset in ONLY_DATASETS: $ds" >&2
      exit 2
    fi
    if ! contains_dataset "$ds" "${SELECTED_DATASETS[@]-}"; then
      SELECTED_DATASETS+=("$ds")
    fi
  done < <(normalize_dataset_list "$ONLY_DATASETS")
else
  SELECTED_DATASETS=("${DEFAULT_DATASETS[@]}")
  if [[ "$INCLUDE_IMDB_CEB_3K" == "1" ]]; then
    SELECTED_DATASETS+=(imdb_ceb_3k)
  fi
fi

SKIPPED_DATASETS=()
if [[ -n "$SKIP_DATASETS" ]]; then
  while IFS= read -r ds; do
    [[ -z "$ds" ]] && continue
    if ! is_valid_dataset "$ds"; then
      echo "[mvp-full] unknown dataset in SKIP_DATASETS: $ds" >&2
      exit 2
    fi
    if ! contains_dataset "$ds" "${SKIPPED_DATASETS[@]-}"; then
      SKIPPED_DATASETS+=("$ds")
    fi
  done < <(normalize_dataset_list "$SKIP_DATASETS")
fi

FINAL_DATASETS=()
for ds in "${SELECTED_DATASETS[@]}"; do
  if contains_dataset "$ds" "${SKIPPED_DATASETS[@]-}"; then
    continue
  fi
  if ! contains_dataset "$ds" "${FINAL_DATASETS[@]-}"; then
    FINAL_DATASETS+=("$ds")
  fi
done

if [[ "${#FINAL_DATASETS[@]}" -eq 0 ]]; then
  echo "[mvp-full] nothing to run after applying ONLY_DATASETS/SKIP_DATASETS"
  exit 0
fi

should_run() {
  local dataset=$1
  contains_dataset "$dataset" "${FINAL_DATASETS[@]-}"
}

echo "[mvp-full] datasets=${FINAL_DATASETS[*]}"
if [[ "$STATEMENT_TIMEOUT_MS" -gt 0 ]]; then
  echo "[mvp-full] statement_timeout_ms=$STATEMENT_TIMEOUT_MS"
fi

run_full() {
  local dataset=$1
  local db=$2
  echo "[mvp-full] full dataset=$dataset db=$db"
  python3 bench/bench.py run "$dataset" "$db" \
    "${COMMON_RUN_ARGS[@]}" \
    "${ALGOS_FULL[@]}"
}

run_no_dp() {
  local dataset=$1
  local db=$2
  echo "[mvp-full] no-dp dataset=$dataset db=$db"
  python3 bench/bench.py run "$dataset" "$db" \
    "${COMMON_RUN_ARGS[@]}" \
    "${ALGOS_NO_DP[@]}"
}

run_dp_capped() {
  local dataset=$1
  local db=$2
  echo "[mvp-full] dp-capped dataset=$dataset db=$db max_join=$DP_MAX_JOIN"
  python3 bench/bench.py run "$dataset" "$db" \
    "${COMMON_RUN_ARGS[@]}" \
    --max-join "$DP_MAX_JOIN" \
    --algo "dp:geqo_threshold=100,enable_goo_join_search=off"
}

# Default MVP matrix intentionally excludes imdb_ceb_3k because it is much larger.
# Set INCLUDE_IMDB_CEB_3K=1 when you explicitly want the heavy CEB run.
if should_run job; then
  run_full job "$IMDB_DB"
fi
if should_run job_complex; then
  run_full job_complex "$IMDB_DB"
fi
if should_run imdb_ceb_3k; then
  run_full imdb_ceb_3k "$IMDB_DB"
else
  if [[ "$INCLUDE_IMDB_CEB_3K" != "1" && -z "$ONLY_DATASETS" ]]; then
    echo "[mvp-full] skip dataset=imdb_ceb_3k (phase-2 heavy workload, enable via INCLUDE_IMDB_CEB_3K=1)"
  fi
fi

if should_run gpuqo_chain_small; then
  run_full gpuqo_chain_small "$GPUQO_CHAIN_DB"
fi
if should_run gpuqo_clique_small; then
  run_full gpuqo_clique_small "$GPUQO_CLIQUE_DB"
fi
if should_run gpuqo_star_small; then
  run_full gpuqo_star_small "$GPUQO_STAR_DB"
fi

if should_run sqlite_select5; then
  run_no_dp sqlite_select5 "$SQLITE_DB"
  run_dp_capped sqlite_select5 "$SQLITE_DB"
fi
if should_run gpuqo_snowflake_small; then
  run_no_dp gpuqo_snowflake_small "$GPUQO_SNOWFLAKE_DB"
  run_dp_capped gpuqo_snowflake_small "$GPUQO_SNOWFLAKE_DB"
fi

echo "[mvp-full] done"
