#!/bin/bash
# ============================================================
# SUBMIT AGE+QUALITY EFFECTS ARRAY (POOLED)
# ============================================================
# Purpose:
#   Submit jobs over the Cartesian product of
#   `microstructural_metrics x (image_quality_metrics + no_quality)`.
# ============================================================

#SBATCH --job-name=age_quality
#SBATCH --output=logs/age_quality_%A_%a.out
#SBATCH --error=logs/age_quality_%A_%a.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --array=1-1386

set -euo pipefail

# Keep numerical backends single-threaded to avoid oversubscription.
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export TMPDIR="${TMPDIR:-$HOME/tmp}"

# Resolve script directory robustly for direct shell runs and sbatch runs.
if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "${SLURM_SUBMIT_DIR}/calculate_age_quality_effects.R" ]; then
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

mkdir -p "$SCRIPT_DIR/logs"
cd "$SCRIPT_DIR"

# Read r_env from config.json using bash tools to avoid extra dependencies.
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

# Compute task-space dimensions from config.
read -r N_METRICS N_QC <<EOF2
$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(length(cfg\$microstructural_metrics), length(cfg\$image_quality_metrics), sep=' ')")
EOF2

TASK_ID="${SLURM_ARRAY_TASK_ID:-1}"
N_QC_ALL=$((N_QC + 1))
TOTAL=$((N_METRICS * N_QC_ALL))
if [ "$TASK_ID" -lt 1 ] || [ "$TASK_ID" -gt "$TOTAL" ]; then
  echo "Error: SLURM_ARRAY_TASK_ID=$TASK_ID is out of range 1-$TOTAL" >&2
  exit 1
fi

METRIC_IDX=$(( (TASK_ID - 1) / N_QC_ALL + 1 ))
QC_IDX_ALL=$(( (TASK_ID - 1) % N_QC_ALL + 1 ))

# Resolve metric and QC names for this array task.
METRIC_NAME="$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(trimws(as.character(cfg\$microstructural_metrics[$METRIC_IDX])))")"
if [ "$QC_IDX_ALL" -le "$N_QC" ]; then
  QC_NAME="$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(trimws(as.character(cfg\$image_quality_metrics[$QC_IDX_ALL])))")"
else
  QC_NAME="no_quality"
fi

if [ -z "$METRIC_NAME" ] || [ -z "$QC_NAME" ]; then
  echo "Error: failed to map TASK_ID=$TASK_ID to metric/qc names" >&2
  exit 1
fi

echo "TASK_ID=$TASK_ID TOTAL=$TOTAL METRIC_IDX=$METRIC_IDX QC_IDX_ALL=$QC_IDX_ALL"
echo "metric=$METRIC_NAME qc_metric=$QC_NAME"

"$RSCRIPT_BIN" "./calculate_age_quality_effects.R" --metric "$METRIC_NAME" --qc_metric "$QC_NAME"
