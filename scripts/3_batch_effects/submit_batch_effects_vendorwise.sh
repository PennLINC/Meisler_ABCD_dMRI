#!/bin/bash
# ============================================================
# SUBMIT BATCH-EFFECTS ARRAY (VENDORWISE, HARMONIZED ONLY)
# ============================================================
# Each array task = ONE (microstructural_metric, qc_metric) combination.
# The R script runs all three vendors (GE, Philips, Siemens) and writes one RDS per vendor.
# Task space: metrics x (image_quality_metrics + no_quality) = same as main batch_effects (1386).
#
# To run only GQI_fa, GQI_md, DKI_mkt, MAPMRI_rtop, NODDI_icvf (5 metrics x 42 = 210 jobs):
#   sbatch --array=253-294,421-462,547-588,1177-1218,1261-1302 submit_batch_effects_vendorwise.sh
#
# To run only no_quality for those 5 metrics (5 jobs):
#   sbatch --array=294,462,588,1218,1302 submit_batch_effects_vendorwise.sh
# ============================================================

#SBATCH --job-name=batch_effects_vendor
#SBATCH --output=logs/batch_effects_vendor_%A_%a.out
#SBATCH --error=logs/batch_effects_vendor_%A_%a.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --array=1-1386

set -euo pipefail

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export TMPDIR="${TMPDIR:-$HOME/tmp}"

if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "${SLURM_SUBMIT_DIR}/calculate_batch_effects_vendorwise.R" ]; then
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

read -r N_METRICS N_QC <<EOF2
$($RSCRIPT_BIN --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); qcs <- c(trimws(as.character(cfg\$image_quality_metrics)), 'no_quality'); cat(length(cfg\$microstructural_metrics), length(qcs), sep=' ')")
EOF2
TOTAL=$((N_METRICS * N_QC))

TASK_ID="${SLURM_ARRAY_TASK_ID:-1}"
if [ "$TASK_ID" -lt 1 ] || [ "$TASK_ID" -gt "$TOTAL" ]; then
  echo "Error: SLURM_ARRAY_TASK_ID=$TASK_ID is out of range 1-$TOTAL" >&2
  exit 1
fi

METRIC_IDX=$(( (TASK_ID - 1) / N_QC + 1 ))
QC_IDX=$(( (TASK_ID - 1) % N_QC + 1 ))

IFS=$'\t' read -r METRIC_NAME QC_NAME < <(
  "$RSCRIPT_BIN" --vanilla -e "cfg <- jsonlite::fromJSON(Sys.getenv('CONFIG_PATH')); qcs <- c(trimws(as.character(cfg\$image_quality_metrics)), 'no_quality'); cat(trimws(as.character(cfg\$microstructural_metrics[$METRIC_IDX])), '\t', trimws(as.character(qcs[$QC_IDX])), '\n', sep='')"
)

if [ -z "$METRIC_NAME" ] || [ -z "$QC_NAME" ]; then
  echo "Error: failed to map TASK_ID=$TASK_ID to metric/qc" >&2
  exit 1
fi

echo "TASK_ID=$TASK_ID metric=$METRIC_NAME qc_metric=$QC_NAME (one job: runs all 3 vendors internally)"
"$RSCRIPT_BIN" "./calculate_batch_effects_vendorwise.R" --metric "$METRIC_NAME" --qc_metric "$QC_NAME"
