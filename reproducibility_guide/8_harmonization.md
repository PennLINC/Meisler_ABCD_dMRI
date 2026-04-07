# Harmonization

This chapter describes how we harmonize diffusion microstructure metrics across scanner/device software batches using longitudinal ComBat-GAM.

The harmonization workflow in this project has two scripts:

1. [scripts/2_harmonize_data/harmonize_data.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/blob/main/scripts/2_harmonize_data/harmonize_data.R)  
Runs one SLURM array task per microstructural metric suffix in `config.json` (`microstructural_metrics`). Each task writes one parquet file.

2. [scripts/2_harmonize_data/assemble_harmonized_data.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/blob/main/scripts/2_harmonize_data/assemble_harmonized_data.R)  
Combines all per-metric harmonized parquet files into a single harmonized analysis dataset.

## Inputs and configuration

The scripts require:

- `CONFIG_PATH` environment variable pointing to your `config.json`
- `${PROJECT_ROOT}/data/raw_data/merged_data_meisler_analyses.parquet` with quality-classifier/manual-rating columns already added (see [Chapter 6: Automated Quality Classification](6_quality_classifier.md))
- `scripts/2_harmonize_data/comfam.R` (sourced by `harmonize_data.R`)

```{note}
Run the [quality-classifier training/deployment workflow](6_quality_classifier.md) before harmonization so the raw analysis parquet already contains `qc_prediction` and related classifier/manual-rating fields used downstream.
```

Key `config.json` field used by harmonization:

- `microstructural_metrics`: vector of metric suffixes to harmonize (for example `DKI_ad`, `GQI_fa`, `NODDI_icvf`)

## How to submit harmonization

Paper-focused submit (main-text 5 metrics only: `DKI_mkt`, `NODDI_icvf`, `MAPMRI_rtop`, `GQI_fa`, `GQI_md`):

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
cd "${PROJECT_ROOT}/scripts/2_harmonize_data"
sbatch --array=7,11,14,29,31 submit_harmonize_data.sh
```

Run everything (all configured microstructural metrics):

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
cd "${PROJECT_ROOT}/scripts/2_harmonize_data"
sbatch submit_harmonize_data.sh
```

`submit_harmonize_data.sh` launches a SLURM array (`--array=1-33`) so that each task harmonizes one feature suffix.

```{note}
The array range should match the length of `microstructural_metrics` in `config.json`.
```

```{note}
When launched via `sbatch`, logs are written under `scripts/2_harmonize_data/logs`.
```

## What `harmonize_data.R` does

For each SLURM task:

1. Reads one metric suffix from `microstructural_metrics` using `SLURM_ARRAY_TASK_ID`.
2. Loads `${PROJECT_ROOT}/data/raw_data/merged_data_meisler_analyses.parquet`.
3. Selects `bundle_..._{feature_suffix}_median` columns (excluding predefined dropped bundles).
4. Builds covariates and batch variables:
   - batch: `batch_device_software`
   - age: `age`
   - sex: `sex`
   - subject ID: `subject_id`
5. Drops rows with missing covariates or missing values in selected target columns.
6. Runs `comfam(...)` (ComBat-GAM via `gamm4`, with random effect `~(1 + age | subject_id)`).
7. Writes harmonized columns with `_harmonized` suffix for that metric task.

Per-task output:

- `${PROJECT_ROOT}/data/harmonized_data/harmonized_parts/harm_{feature_suffix}.parquet`

Each output file contains:

- keys: `subject_id`, `session_id`
- harmonized bundle columns ending in `_harmonized`

## Assembling harmonized outputs

After array jobs finish, run:

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
R_ENV=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["r_env"])' "$CONFIG_PATH")
"${R_ENV}/bin/Rscript" "${PROJECT_ROOT}/scripts/2_harmonize_data/assemble_harmonized_data.R"
```

`assemble_harmonized_data.R`:

1. Loads `${PROJECT_ROOT}/data/raw_data/merged_data_meisler_analyses.parquet`.
2. Loads all available `harm_*.parquet` files expected from `microstructural_metrics`.
3. Renames harmonized columns by removing `_harmonized` suffix.
4. Drops raw `bundle_` columns from the base dataframe.
5. Left-joins harmonized bundle columns back by `subject_id` + `session_id`.

Final assembled output:

- `${PROJECT_ROOT}/data/harmonized_data/merged_data_meisler_analyses_harmonized.parquet`

### Typical failure points

- `CONFIG_PATH` not set or pointing to the wrong file
- `merged_data_meisler_analyses.parquet` missing (filtering step not completed)
- Missing `harm_*.parquet` parts because array jobs failed or array range mismatched `microstructural_metrics`
- Missing `ComBatFamily` branch required for the longitudinal `comfam`
