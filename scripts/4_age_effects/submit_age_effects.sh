#!/bin/bash
# ============================================================
# SUBMIT AGE EFFECTS ARRAY (POOLED, HARMONIZED ONLY)
# ============================================================
# Purpose:
#   Submit jobs over metric x (image_quality_metrics + no_quality).
#   Task space: 33 x 42 = 1386. Full: y ~ s(age,k=4) + sex + s(qc,k=4) or + sex; reduced drops s(age).
#
#   To run only GQI_fa, GQI_md, DKI_mkt, MAPMRI_rtop, NODDI_icvf (5 metrics x 42 = 210 jobs):
#   sbatch --array=253-294,421-462,547-588,1177-1218,1261-1302 submit_age_effects.sh
#
#   To run only qc_prediction for the 5 focus metrics (5 jobs):
#   sbatch --array=293,461,587,1217,1301 submit_age_effects.sh
# ============================================================

#SBATCH --job-name=age_effects
#SBATCH --output=logs/age_effects_%A_%a.out
#SBATCH --error=logs/age_effects_%A_%a.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --array=1-1386

set -euo pipefail

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export TMPDIR="${TMPDIR:-$HOME/tmp}"

if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "${SLURM_SUBMIT_DIR}/calculate_age_effects.R" ]; then
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

read -r N_METRICS N_QC <<EOF2
$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(length(cfg\$microstructural_metrics), length(cfg\$image_quality_metrics), sep=' ')")
EOF2
N_QC_ALL=$((N_QC + 1))
TOTAL=$((N_METRICS * N_QC_ALL))

TASK_ID="${SLURM_ARRAY_TASK_ID:-1}"
if [ "$TASK_ID" -lt 1 ] || [ "$TASK_ID" -gt "$TOTAL" ]; then
  echo "Error: SLURM_ARRAY_TASK_ID=$TASK_ID is out of range 1-$TOTAL" >&2
  exit 1
fi

METRIC_IDX=$(( (TASK_ID - 1) / N_QC_ALL + 1 ))
QC_IDX=$(( (TASK_ID - 1) % N_QC_ALL + 1 ))

METRIC_NAME="$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(trimws(as.character(cfg\$microstructural_metrics[$METRIC_IDX])))")"
if [ "$QC_IDX" -le "$N_QC" ]; then
  QC_NAME="$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); cat(trimws(as.character(cfg\$image_quality_metrics[$QC_IDX])))")"
else
  QC_NAME="no_quality"
fi

if [ -z "$METRIC_NAME" ] || [ -z "$QC_NAME" ]; then
  echo "Error: failed to resolve metric/qc for TASK_ID=$TASK_ID" >&2
  exit 1
fi

echo "TASK_ID=$TASK_ID metric=$METRIC_NAME qc_metric=$QC_NAME"
"$RSCRIPT_BIN" "./calculate_age_effects.R" --metric "$METRIC_NAME" --qc_metric "$QC_NAME"
