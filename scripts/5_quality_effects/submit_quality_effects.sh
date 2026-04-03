#!/bin/bash
# ============================================================
# SUBMIT QUALITY EFFECTS ARRAY (POOLED, HARMONIZED ONLY)
# ============================================================
# Purpose:
#   Submit jobs over metric x image_quality_metrics only (no no_quality).
#   Task space: 33 x 41 = 1353. Full: y ~ s(age,k=4) + sex + s(qc,k=4) + batch; reduced drops s(qc).
#
#   To run only GQI_fa, GQI_md, DKI_mkt, MAPMRI_rtop, NODDI_icvf (5 metrics x 41 = 205 jobs):
#   sbatch --array=247-287,411-451,534-574,1149-1189,1231-1271 submit_quality_effects.sh
#
#   To run only qc_prediction for the 5 focus metrics (5 jobs):
#   sbatch --array=287,451,574,1189,1271 submit_quality_effects.sh
# ============================================================

#SBATCH --job-name=quality_effects
#SBATCH --output=logs/quality_effects_%A_%a.out
#SBATCH --error=logs/quality_effects_%A_%a.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --array=1-1353

set -euo pipefail

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export TMPDIR="${TMPDIR:-$HOME/tmp}"

if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "${SLURM_SUBMIT_DIR}/calculate_quality_effects.R" ]; then
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

# Task space: N_METRICS x N_QC (image_quality_metrics only, no no_quality)
read -r N_METRICS N_QC <<EOF2
$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(length(cfg\$microstructural_metrics), length(cfg\$image_quality_metrics), sep=' ')")
EOF2
TOTAL=$((N_METRICS * N_QC))

TASK_ID="${SLURM_ARRAY_TASK_ID:-1}"
if [ "$TASK_ID" -lt 1 ] || [ "$TASK_ID" -gt "$TOTAL" ]; then
  echo "Error: SLURM_ARRAY_TASK_ID=$TASK_ID is out of range 1-$TOTAL" >&2
  exit 1
fi

METRIC_IDX=$(( (TASK_ID - 1) / N_QC + 1 ))
QC_IDX=$(( (TASK_ID - 1) % N_QC + 1 ))

METRIC_NAME="$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(trimws(as.character(cfg\$microstructural_metrics[$METRIC_IDX])))")"
QC_NAME="$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(trimws(as.character(cfg\$image_quality_metrics[$QC_IDX])))")"

if [ -z "$METRIC_NAME" ] || [ -z "$QC_NAME" ]; then
  echo "Error: failed to map TASK_ID=$TASK_ID to metric/qc names" >&2
  exit 1
fi

echo "TASK_ID=$TASK_ID metric=$METRIC_NAME qc_metric=$QC_NAME"
"$RSCRIPT_BIN" "./calculate_quality_effects.R" --metric "$METRIC_NAME" --qc_metric "$QC_NAME"
