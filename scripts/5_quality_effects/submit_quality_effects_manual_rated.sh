#!/bin/bash
# ============================================================
# SUBMIT QUALITY EFFECTS (MANUALLY RATED, SIEMENS ONLY)
# ============================================================
# Purpose:
#   One job per microstructural metric. Each job fits GAMs on subset
#   manuall_rated == TRUE, scanner_manufacturer == "Siemens", with quality
#   covariates t1post_dwi_contrast and mean_rating; quality effect = Δ adjusted R².
# ============================================================

#SBATCH --job-name=quality_manual
#SBATCH --output=logs/quality_effects_manual_rated_%A_%a.out
#SBATCH --error=logs/quality_effects_manual_rated_%A_%a.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --array=1-33   # one per microstructural_metric in config; update if config length changes

set -euo pipefail

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export TMPDIR="${TMPDIR:-$HOME/tmp}"

if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "${SLURM_SUBMIT_DIR}/calculate_quality_effects_manual_rated.R" ]; then
  SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
fi

if [ -z "${CONFIG_PATH:-}" ]; then
  echo "Error: CONFIG_PATH environment variable is not set." >&2
  exit 1
fi

if [ ! -f "$CONFIG_PATH" ]; then
  echo "Error: config file not found at $CONFIG_PATH" >&2
  exit 1
fi
export CONFIG_PATH

mkdir -p "$SCRIPT_DIR/logs"
cd "$SCRIPT_DIR"

R_ENV="$(
  sed -nE 's/^[[:space:]]*"r_env"[[:space:]]*:[[:space:]]*"([^"]+)".*$/\1/p' "$CONFIG_PATH" \
    | head -n 1
)"

if [ -z "$R_ENV" ]; then
  echo "Error: Could not read r_env from $CONFIG_PATH" >&2
  exit 1
fi

RSCRIPT_BIN="$R_ENV/bin/Rscript"
if [ ! -x "$RSCRIPT_BIN" ]; then
  echo "Error: executable not found at $RSCRIPT_BIN" >&2
  exit 1
fi

N_METRICS="$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(length(cfg\$microstructural_metrics))")"
TOTAL="$N_METRICS"

TASK_ID="${SLURM_ARRAY_TASK_ID:-1}"
if [ "$TASK_ID" -lt 1 ] || [ "$TASK_ID" -gt "$TOTAL" ]; then
  echo "Error: SLURM_ARRAY_TASK_ID=$TASK_ID is out of range 1-$TOTAL" >&2
  exit 1
fi

METRIC_NAME="$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(trimws(as.character(cfg\$microstructural_metrics[$TASK_ID])))")"

if [ -z "$METRIC_NAME" ]; then
  echo "Error: failed to resolve metric for TASK_ID=$TASK_ID" >&2
  exit 1
fi

echo "TASK_ID=$TASK_ID metric=$METRIC_NAME"
"$RSCRIPT_BIN" "./calculate_quality_effects_manual_rated.R" --metric "$METRIC_NAME"
