# Modeling

This chapter documents the modeling scripts used after harmonization.  
The sections below follow the requested order.

All scripts assume:

- `CONFIG_PATH` points to your `config.json`
- `config.json` defines `project_root`, `microstructural_metrics`, and `image_quality_metrics`

## Main-text efficient run plan

For main-text analyses, we use only these 5 microstructural metrics:

- `DKI_mkt`
- `NODDI_icvf`
- `MAPMRI_rtop`
- `GQI_fa`
- `GQI_md`

Although the commands are designed to `sbatch` jobs for every microstructure/IQM combination, we can also limit which IQMs are analyzed for efficiency.

Quality-covariate usage in main figures:

- Batch-effects figure uses `qc_covariate == "no_quality"` only.
- Quality-effects figures do use quality covariates:
  - Figure 6/7 pooled quality effects use IQM-based outputs from `quality_effects_all_outputs.rds`.
  - Figure 6 manual-rated panel uses `t1post_dwi_contrast` and `mean_rating`.
  - Figure 7 GE row uses vendorwise age effects with `no_quality` and selected IQMs (`t1post_dwi_contrast`, `mean_fd`, `qc_prediction`, `t1post_neighbor_corr`).

```{note}
Note that these sbatch jobs in this script can all be run simultaneously! Feel free to start them all and check back in a couple hours.
```

## 1) Batch effects

### `calculate_batch_effects.R`

Script: [scripts/3_batch_effects/calculate_batch_effects.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/3_batch_effects/calculate_batch_effects.R)

Purpose:

- Runs one task per `(metric, qc_metric)` pair.
- For each bundle, compares:
  - Full model: `value ~ s(age) + sex + s(qc_var) + batch_device_software` (or without `s(qc_var)` for `no_quality`)
  - Reduced model: drops `batch_device_software`
- Computes batch effect size as `delta R^2 = r2_full - r2_reduced`.
- Runs on both raw and harmonized datasets (`source = raw|harmonized`).

Inputs:

- `${PROJECT_ROOT}/data/raw_data/merged_data_meisler_analyses.parquet`
- `${PROJECT_ROOT}/data/harmonized_data/merged_data_meisler_analyses_harmonized.parquet`

Output (per task):

- `${PROJECT_ROOT}/data/batch_effects/batch_effects_outputs/{metric}__{qc}_batch_effects.rds`

Paper-focused submit (`no_quality` for the 5 paper metrics only):

```bash
cd "${PROJECT_ROOT}/scripts/3_batch_effects"
sbatch --array=294,462,588,1218,1302 submit_batch_effects.sh
```

Alternate: run everything in this script:

```bash
cd "${PROJECT_ROOT}/scripts/3_batch_effects"
sbatch submit_batch_effects.sh
```

### `assemble_batch_effects.R`

Script: [scripts/3_batch_effects/assemble_batch_effects.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/3_batch_effects/assemble_batch_effects.R)

Purpose:

- Combines pooled outputs and vendorwise outputs (if present) into one table.

Input directories:

- `${PROJECT_ROOT}/data/batch_effects/batch_effects_outputs`
- `${PROJECT_ROOT}/data/batch_effects/batch_effects_outputs_vendorwise` (optional)

Final output:

- `${PROJECT_ROOT}/data/batch_effects/batch_effects_all_outputs.rds`

Run:

```bash
Rscript "${PROJECT_ROOT}/scripts/3_batch_effects/assemble_batch_effects.R"
```

## 2) Age effects

### `calculate_age_effects.R`

Script: [scripts/4_age_effects/calculate_age_effects.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/4_age_effects/calculate_age_effects.R)

Purpose:

- Runs pooled age-effect models on harmonized data only.
- One task per `(metric, qc_metric)` where `qc_metric` includes image-quality metrics plus `no_quality`.
- Age effect is computed by dropping `s(age)` from the reduced model.

Input:

- `${PROJECT_ROOT}/data/harmonized_data/merged_data_meisler_analyses_harmonized.parquet`

Output (per task):

- `${PROJECT_ROOT}/data/age_effects/age_effects_outputs/{metric}__{qc}_age_effects.rds`

Paper-focused pooled submit (`no_quality`, 5 paper metrics):

```bash
cd "${PROJECT_ROOT}/scripts/4_age_effects"
sbatch --array=294,462,588,1218,1302 submit_age_effects.sh
```

Alternate: run everything in this script:

```bash
cd "${PROJECT_ROOT}/scripts/4_age_effects"
sbatch submit_age_effects.sh
```

### `calculate_age_effects_vendorwise.R`

Script: [scripts/4_age_effects/calculate_age_effects_vendorwise.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/4_age_effects/calculate_age_effects_vendorwise.R)

Purpose:

- Runs age-effect models separately for `GE`, `Philips`, and `Siemens`.
- Uses both raw and harmonized data.
- One task per `(metric, qc_metric)`; each task writes one file per vendor.

Inputs:

- `${PROJECT_ROOT}/data/raw_data/merged_data_meisler_analyses.parquet`
- `${PROJECT_ROOT}/data/harmonized_data/merged_data_meisler_analyses_harmonized.parquet`

Outputs (per task/vendor):

- `${PROJECT_ROOT}/data/age_effects/age_effects_outputs_vendorwise/{metric}__{qc}__{vendor}_age_effects_by_vendor.rds`

Paper-focused vendorwise submits:

```bash
cd "${PROJECT_ROOT}/scripts/4_age_effects"

# Figure 5 vendorwise panel (no_quality, 5 paper metrics)
sbatch --array=294,462,588,1218,1302 submit_age_effects_vendorwise.sh

# Figure 7 GE row (GQI_fa with selected IQMs)
sbatch --array=433,435,439,461 submit_age_effects_vendorwise.sh
```

Alternate: run everything in this script:

```bash
cd "${PROJECT_ROOT}/scripts/4_age_effects"
sbatch submit_age_effects_vendorwise.sh
```

### `assemble_age_effects.R`

Script: [scripts/4_age_effects/assemble_age_effects.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/4_age_effects/assemble_age_effects.R)

Purpose:

- Combines pooled and vendorwise age-effect RDS files.

Input directories:

- `${PROJECT_ROOT}/data/age_effects/age_effects_outputs`
- `${PROJECT_ROOT}/data/age_effects/age_effects_outputs_vendorwise`

Final output:

- `${PROJECT_ROOT}/data/age_effects/age_effects_all_outputs.rds`

Run:

```bash
Rscript "${PROJECT_ROOT}/scripts/4_age_effects/assemble_age_effects.R"
```

## 3) Quality effects (automated IQMs)

### `calculate_quality_effects.R`

Script: [scripts/5_quality_effects/calculate_quality_effects.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/5_quality_effects/calculate_quality_effects.R)

Purpose:

- Runs pooled quality-effect models on harmonized data only.
- One task per `(metric, qc_metric)` using `image_quality_metrics` only (no `no_quality` task).
- Full model includes `s(qc_var)` and `batch_device_software`; reduced model drops `s(qc_var)`.
- Quality effect is `delta R^2 = r2_full - r2_reduced_qc`.

Input:

- `${PROJECT_ROOT}/data/harmonized_data/merged_data_meisler_analyses_harmonized.parquet`

Output (per task):

- `${PROJECT_ROOT}/data/quality_effects/quality_effects_outputs/{metric}__{qc}_quality_effects.rds`

Paper-focused pooled submit (all IQMs for the 5 paper metrics):

```bash
cd "${PROJECT_ROOT}/scripts/5_quality_effects"
sbatch --array=247-287,411-451,534-574,1149-1189,1231-1271 submit_quality_effects.sh
```

Alternate: run everything in this script:

```bash
cd "${PROJECT_ROOT}/scripts/5_quality_effects"
sbatch submit_quality_effects.sh
```

### `assemble_quality_effects.R`

Script: [scripts/5_quality_effects/assemble_quality_effects.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/5_quality_effects/assemble_quality_effects.R)

Purpose:

- Combines pooled quality-effect outputs and vendorwise outputs (if present).

Input directories:

- `${PROJECT_ROOT}/data/quality_effects/quality_effects_outputs`
- `${PROJECT_ROOT}/data/quality_effects/quality_effects_outputs_vendorwise` (optional)

Final output:

- `${PROJECT_ROOT}/data/quality_effects/quality_effects_all_outputs.rds`

Run:

```bash
Rscript "${PROJECT_ROOT}/scripts/5_quality_effects/assemble_quality_effects.R"
```

## 4) Quality effects (manual-rated subset)

### `calculate_quality_effects_manual_rated.R`

Script: [scripts/5_quality_effects/calculate_quality_effects_manual_rated.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/5_quality_effects/calculate_quality_effects_manual_rated.R)

Purpose:

- Restricts to `manually_rated == TRUE`.
- Runs GAM-based quality-effect analyses (no random effects) for vendors `GE`, `Philips`, `Siemens`, and `pooled`.
- Evaluates two quality covariates:
  - `t1post_dwi_contrast`
  - `mean_rating`
- For each bundle and covariate, quality effect is change in adjusted R² relative to reduced model without quality covariate.
- One task per microstructural metric.

Input:

- `${PROJECT_ROOT}/data/harmonized_data/merged_data_meisler_analyses_harmonized.parquet`

Output (per metric):

- `${PROJECT_ROOT}/data/quality_effects/quality_effects_manual_rated_outputs/{metric}_quality_effects_manual_rated.rds`

Paper-focused submit (5 paper metrics only):

```bash
cd "${PROJECT_ROOT}/scripts/5_quality_effects"
sbatch --array=7,11,14,29,31 submit_quality_effects_manual_rated.sh
```

Alternate: run everything in this script:

```bash
cd "${PROJECT_ROOT}/scripts/5_quality_effects"
sbatch submit_quality_effects_manual_rated.sh
```

### `assemble_quality_effects_manual_rated.R`

Script: [scripts/5_quality_effects/assemble_quality_effects_manual_rated.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/5_quality_effects/assemble_quality_effects_manual_rated.R)

Purpose:

- Concatenates all per-metric manual-rated quality-effect RDS files.

Input directory:

- `${PROJECT_ROOT}/data/quality_effects/quality_effects_manual_rated_outputs`

Final output:

- `${PROJECT_ROOT}/data/quality_effects/quality_effects_manual_rated_all_outputs.rds`

Run:

```bash
Rscript "${PROJECT_ROOT}/scripts/5_quality_effects/assemble_quality_effects_manual_rated.R"
```
