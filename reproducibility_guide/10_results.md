# Results Text And Table Generation

This chapter describes the notebooks in `scripts/7_results` that generate the paragraph-ready statistics used in the main-text Results and Table 1.

## Setup

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
PY_ENV=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["python_env"])' "$CONFIG_PATH")
```

Open and run interactively (recommended):

```bash
cd "${PROJECT_ROOT}"
"${PY_ENV}/bin/jupyter" lab "scripts/7_results"
```

```{note}
Launch Jupyter from `${PROJECT_ROOT}` so notebook `config.json` fallback resolution (searching upward from working directory) works when `CONFIG_PATH` is unset.
```

```{note}
For results notebooks, interactive execution is usually best because the key outputs are rendered notebook tables/text blocks rather than standalone files.
```

## Notebook-to-figure mapping

- `Table1.ipynb` -> Main text Table 1
- `Results_paragraph_IQM.ipynb` -> Figure 2
- `Results_paragraph_vendor_harmonization.ipynb` -> Figure 3
- `Results_paragraph_age_effects.ipynb` -> Figure 4
- `Results_paragraph_cross_vendor_correspondence.ipynb` -> Figure 5A-C
- `Results_paragraph_cross_metric_correspondence.ipynb` -> Figure 5D-F
- `Results_paragraph_quality_effects.ipynb` -> Figure 6
- `Results_paragraph_batch_age_quality.ipynb` -> Figure 7

## What statistics are run (and why)

### `Table1.ipynb` (Table 1)

- Statistics run:
  - Descriptive counts by session and manufacturer (Siemens/GE/Philips).
  - Session-wise age mean and SD.
  - Session-wise sex counts/proportions.
  - Overall totals across sessions.
- Why:
  - Provides cohort composition/context for interpreting all downstream modeling results.

### `Results_paragraph_IQM.ipynb` (Figure 2)

- Statistics run:
  - Kruskal-Wallis tests across vendors for each IQM, with `eta^2` effect size.
  - Pairwise Wilcoxon rank-sum tests between vendors, with rank-biserial `r`.
  - Paired Wilcoxon signed-rank tests for raw vs preprocessed NDC and dMRI contrast, with paired effect-size `r`.
  - Additional within-GE software-version Kruskal-Wallis and pairwise Wilcoxon comparisons.
- Why:
  - Quantifies vendor/software heterogeneity and validates preprocessing-related IQM shifts shown in Figure 2.

### `Results_paragraph_vendor_harmonization.ipynb` (Figure 3)

- Statistics run:
  - Batch effect size summaries from assembled outputs where
    - `batch_effect = Delta R^2_adj(full model) - Delta R^2_adj(model without batch_device_software)`.
  - Raw vs harmonized summaries (min/max/mean across bundles, metric-specific summaries).
- Why:
  - Tests whether longitudinal harmonization reduces scanner batch-driven variance and by how much.

### `Results_paragraph_age_effects.ipynb` (Figure 4)

- Statistics run:
  - Summary distributions of bundle-wise age effect sizes (`Delta R^2_adj`) by metric in harmonized pooled models (`qc_metric = no_quality`).
  - Metric-level ranges and means, plus category-level patterns.
- Why:
  - Identifies which microstructural metrics are most developmentally sensitive in the main analyses.

### `Results_paragraph_cross_vendor_correspondence.ipynb` (Figure 5A-C)

- Statistics run:
  - Spearman `rho` between vendor-pair vectors of bundle-wise age effects.
  - Computed for each metric in raw and harmonized data.
  - `Delta rho` (harmonized minus raw) and largest-gain case extraction.
- Why:
  - Quantifies harmonization-driven improvements in cross-vendor generalizability/reproducibility.

### `Results_paragraph_cross_metric_correspondence.ipynb` (Figure 5D-F)

- Statistics run:
  - Spearman `rho` between metric-pair vectors of bundle-wise age effects (pooled harmonized).
  - Identifies strongest and weakest metric-pair agreement.
- Why:
  - Evaluates consistency of developmental spatial patterns across diffusion models.

### `Results_paragraph_quality_effects.ipynb` (Figure 6)

- Statistics run:
  - Winner-take-all quality analysis: for each bundle-metric pair, IQM with largest `qc_effect_size` (`Delta R^2_adj`), then counts of IQM "wins".
  - Metric-specific min/max/mean quality-effect summaries for focal IQMs (especially `t1post_dwi_contrast`).
  - Pearson correlation (`cor.test`) between manual mean rating and preprocessed dMRI contrast in manually rated scans.
  - Manual-vs-contrast effect comparison in manual subset (`t1post_dwi_contrast - mean_rating` differences).
  - Paired Wilcoxon effect summary for FA age effects without vs with dMRI contrast covariate (contextual tie-in to downstream sensitivity).
- Why:
  - Determines which quality covariates are most informative, and whether automated contrast aligns with manual quality ratings.

### `Results_paragraph_batch_age_quality.ipynb` (Figure 7)

- Statistics run:
  - GE-specific GAM models (`mgcv`) per IQM:
    - age-only model,
    - batch-only model,
    - age+batch model.
  - Derived mediation-style quantity: proportion of age-IQM variance attributable to batch structure.
  - Spearman `rho` between FA bundle-wise age effects without QC covariate vs with each IQM covariate.
- Why:
  - Separates true developmental signal from batch-linked IQM structure and quantifies how QC-covariate choice changes developmental inference.

## Inputs used by results notebooks

Most results notebooks use these assembled files from earlier chapters:

- `data/age_effects/age_effects_all_outputs.rds`
- `data/batch_effects/batch_effects_all_outputs.rds`
- `data/quality_effects/quality_effects_all_outputs.rds`
- `data/quality_effects/quality_effects_manual_rated_all_outputs.rds`
- `data/harmonized_data/merged_data_meisler_analyses_harmonized.parquet`

The notebooks are designed to print manuscript-ready numbers and draft paragraph text directly in notebook cells.
