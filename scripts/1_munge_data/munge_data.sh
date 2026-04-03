#!/bin/bash
# ============================================================
# SUBMIT MUNGE-DATA PIPELINE
# ============================================================
# Purpose:
#   Run the single-step raw-data merge script (`munge_data.R`) using the
#   R environment configured in `config.json`.
# ============================================================

#SBATCH --job-name=munge_data
#SBATCH --output=logs/munge_data_%j.out
#SBATCH --error=logs/munge_data_%j.err
#SBATCH --time=47:59:59
#SBATCH --mem-per-cpu=16GB
#SBATCH --cpus-per-task=16

set -euo pipefail

# Keep numerical backends single-threaded to avoid oversubscription.
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export TMPDIR="${TMPDIR:-$HOME/tmp}"

# Resolve script directory robustly for direct shell runs and sbatch runs.
if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "${SLURM_SUBMIT_DIR}/munge_data.R" ]; then
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

# Run the pipeline entrypoint.
"$RSCRIPT_BIN" "./munge_data.R"
