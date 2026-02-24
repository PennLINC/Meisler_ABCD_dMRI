#!/bin/bash
# ============================================================
# SUBMIT HARMONIZATION ARRAY
# ============================================================
# Purpose:
#   Submit harmonization jobs where each array task harmonizes one
#   microstructural metric (task mapping is handled in harmonize_data.R).
# ============================================================

#SBATCH --job-name=combat_harmonize
#SBATCH --output=logs/combat_harmonize_%A_%a.out
#SBATCH --error=logs/combat_harmonize_%A_%a.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --array=1-33

set -euo pipefail

# Keep numerical backends single-threaded to avoid oversubscription.
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export TMPDIR="${TMPDIR:-$HOME/tmp}"

# Resolve script directory robustly for direct shell runs and sbatch runs.
if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "${SLURM_SUBMIT_DIR}/harmonize_data.R" ]; then
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

"$RSCRIPT_BIN" "./harmonize_data.R"
