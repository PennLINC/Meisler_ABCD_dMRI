# Automated Quality Classification

This chapter describes the post-rating automated quality-classifier workflow.

Run this workflow **after** manual ratings and **before** harmonization.

Reason: these scripts append/refresh quality-classifier columns in the raw analysis parquet (for example `qc_prediction`, `qc_pass_binary`, `manually_rated`, `qc_prediction_source`, and `qc_prediction_cv_mean`).

## Inputs expected

- `${PROJECT_ROOT}/data/raw_data/merged_data_meisler_analyses.parquet`
- Manual ratings in `mean_rating` (or precomputed `mean_rating_scaled` / `rating_binary`)

If needed, classifier scripts derive targets as:

- `mean_rating_scaled = (mean_rating + 2) / 4`
- `rating_binary = mean_rating_scaled >= 0.5`

## 1. Nested CV over seeds (performance + SHAP)

Script: `scripts/quality_classifier/automated_classification/cross_validate_model.py` ([GitHub](https://github.com/PennLINC/Meisler_ABCD_dMRI/blob/main/scripts/quality_classifier/automated_classification/cross_validate_model.py))

Core mechanics:

- 5x5 nested CV (`n_outer_splits=5`, `n_inner_splits=5`)
- Bayesian hyperparameter search (`n_iter=200`) in each outer fold
- XGBoost regressor with raw IQMs (no harmonization/scaling in this script)
- Default seed schedule supports up to `n_seeds=1000`
- Stratification combines binary target and manufacturer batch when possible (falls back to binary-only if small cells)

Local single-seed example:

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
PY_ENV=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["python_env"])' "$CONFIG_PATH")

"${PY_ENV}/bin/python" "${PROJECT_ROOT}/scripts/quality_classifier/automated_classification/cross_validate_model.py" --seed 0
```

Outputs (default):

- `${PROJECT_ROOT}/data/quality_classifier/cross_validation_results/preds_ALL_<seed>.csv`
- `${PROJECT_ROOT}/data/quality_classifier/cross_validation_results/metrics_ALL_<seed>.csv`
- `${PROJECT_ROOT}/data/quality_classifier/cross_validation_results/shap_ALL_<seed>.csv`
- `${PROJECT_ROOT}/data/quality_classifier/cross_validation_results/shap_ranks_ALL_<seed>.csv`

SLURM-array pattern (example):

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
PY_ENV=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["python_env"])' "$CONFIG_PATH")

sbatch --array=0-999 --wrap "\"${PY_ENV}/bin/python\" \"${PROJECT_ROOT}/scripts/quality_classifier/automated_classification/cross_validate_model.py\""
```

## 2. Train final pooled model

Script: `scripts/quality_classifier/automated_classification/final_model_training.py` ([GitHub](https://github.com/PennLINC/Meisler_ABCD_dMRI/blob/main/scripts/quality_classifier/automated_classification/final_model_training.py))

Core mechanics:

- Trains on all rated rows that pass feature/target completeness checks
- Hyperparameter tuning: 5-fold BayesSearchCV (`n_iter=200`, default `seed=42`)
- Saves best estimator and best params
- Reports cross-validated pooled and per-batch metrics

Run:

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
PY_ENV=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["python_env"])' "$CONFIG_PATH")

"${PY_ENV}/bin/python" "${PROJECT_ROOT}/scripts/quality_classifier/automated_classification/final_model_training.py"
```

Outputs (default):

- `${PROJECT_ROOT}/data/quality_classifier/final_pooled_model/final_model_all.joblib`
- `${PROJECT_ROOT}/data/quality_classifier/final_pooled_model/final_model_best_params.json`
- `${PROJECT_ROOT}/data/quality_classifier/final_pooled_model/final_model_metrics.csv`

## 3. Deploy model predictions into raw analysis parquet

Script: `scripts/quality_classifier/automated_classification/deploy_model.py` ([GitHub](https://github.com/PennLINC/Meisler_ABCD_dMRI/blob/main/scripts/quality_classifier/automated_classification/deploy_model.py))

This script predicts on valid rows, uses CV out-of-fold means for rated scans when available, and writes classifier columns to raw-data parquet.

Recommended (overwrite analysis parquet used downstream):

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
PY_ENV=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["python_env"])' "$CONFIG_PATH")

"${PY_ENV}/bin/python" "${PROJECT_ROOT}/scripts/quality_classifier/automated_classification/deploy_model.py" \
  --out-path raw_data/merged_data_meisler_analyses.parquet
```

If you omit `--out-path`, deployment overwrites:

- `${PROJECT_ROOT}/data/raw_data/merged_data_meisler_analyses.parquet`

but then you must explicitly point downstream steps to that file (or replace the canonical `merged_data_meisler_analyses.parquet`) before harmonization.
