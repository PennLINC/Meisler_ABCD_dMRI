# Figures (Main Text)

Figure 1 was created entirely in Illustrator, so there is no code to run for that figure.

This chapter covers Figures 2–7 in `scripts/6_figures`.

## Prerequisites

Before running figure notebooks, make sure these steps are complete:

- Data munging/filtering (`merged_data_meisler_analyses.parquet`)
- Harmonization (`merged_data_meisler_analyses_harmonized.parquet`)
- Modeling outputs assembled:
  - `data/batch_effects/batch_effects_all_outputs.rds`
  - `data/age_effects/age_effects_all_outputs.rds`
  - `data/quality_effects/quality_effects_all_outputs.rds`
  - `data/quality_effects/quality_effects_manual_rated_all_outputs.rds` (for manual-rated panels)

## Running the notebooks

All figure notebooks use the `r` kernel.

Option A: open and run interactively (recommended):

```bash
jupyter lab "${PROJECT_ROOT}/scripts/6_figures"
```

Option B: execute from CLI (headless):

```bash
cd "${PROJECT_ROOT}/scripts/6_figures"
"${PY_ENV}/bin/jupyter" nbconvert --to notebook --execute --inplace Figure2.ipynb
```

Run all main-text figure notebooks via CLI:

```bash
for nb in Figure2.ipynb Figure3.ipynb Figure4.ipynb Figure5.ipynb Figure6.ipynb Figure7.ipynb; do
  "${PY_ENV}/bin/jupyter" nbconvert --to notebook --execute --inplace "$nb"
done
```

Figures are saved under `${PROJECT_ROOT}/figures/FigureX` with panel-level files in `panels/`.

## What each figure notebook does

### Figure 2 (`Figure2.ipynb`)

- Purpose: establish scanner/vendor heterogeneity in IQMs, and show how preprocessing changes IQMs.
- Main panels produced in notebook:
  - Panels A/B: vendor-colored scatter + marginals for preprocessed NDC and preprocessed dMRI contrast.
  - Panels C/D: vendor distributions for shell-wise CNR and b=0 tSNR.
  - Panel E: age distributions across software versions (within vendor, parsed from `batch_device_software`).
  - Panel F: within-vendor software-version boxplots for preprocessed NDC and preprocessed dMRI contrast.
- Interpretation in main text: preprocessing improves quality metrics overall, but substantial vendor/software differences remain.

### Figure 3 (`Figure3.ipynb`)

- Purpose: quantify scanner batch/device-software effects and verify attenuation after harmonization.
- Main panels:
  - Panel A: bundle-level heatmap of batch effect size (`ΔR²adj`) in unharmonized data.
  - Panels B/C: metric-level summaries (raw vs harmonized), showing mean/range reduction of batch susceptibility.
- Interpretation in main text: strong pre-harmonization batch structure is substantially reduced after longitudinal harmonization.

### Figure 4 (`Figure4.ipynb`)

- Purpose: summarize developmental sensitivity of each microstructural metric across bundles.
- Main panels:
  - Panel A: heatmap of age effect size (`ΔR²adj`) across bundles and metrics.
  - Panel B: focused ICVF age-effect visualization (strong developmental sensitivity).
  - Panel C: focused MKT/RTOP visualizations for non-Gaussian/advanced metrics.
- Interpretation in main text: advanced metrics (especially ICVF, then MKT/RTOP) show stronger developmental effects than tensor-derived measures.

### Figure 5 (`Figure5.ipynb`)

- Purpose: test generalizability of age effects across vendors and across diffusion metrics.
- Main panels:
  - Panel A: cross-vendor Spearman `rho` (unharmonized vs harmonized) by metric.
  - Panels B/C: vendor-pair bundle scatter before/after harmonization for the largest-gain case.
  - Panel D: cross-metric correspondence heatmap (pooled harmonized age effects).
  - Panels E/F: best-pair and worst-pair metric correspondence scatters.
- Interpretation in main text: harmonization improves cross-vendor reproducibility, and advanced metrics have higher cross-metric agreement.

### Figure 6 (`Figure6.ipynb`)

- Purpose: identify which IQMs most strongly explain microstructural variance, and compare automated vs manual quality signals.
- Main panels:
  - Panel A: winner-take-all map/counts of IQMs with largest quality effect (`qc_effect_size`) per bundle-metric combination.
  - Panels B/C: preprocessed dMRI contrast susceptibility summaries by metric/bundle.
  - Panel D: manually rated subset scatter (`mean_rating` vs `t1post_dwi_contrast`) with marginals.
  - Panel E: violin of `t1post_dwi_contrast - mean_rating` quality-effect size (`ΔR²adj`) in manually rated subset.
- Interpretation in main text: dMRI contrast is a robust and comparatively less batch-confounded quality covariate.

### Figure 7 (`Figure7.ipynb`)

- Purpose: integrate batch-age-quality relationships and show downstream impact on developmental FA effects.
- Main panels:
  - Panel A: mediation-style summary of age-IQM association attributable to batch (linear/GAM variants).
  - Panel B: paired bars combining winner-take-all context with GAM mediation outputs.
  - Panel C: FA age-effect comparison with vs without quality covariates.
  - Panel D: GE-only 4x3 grid:
    - Row 1: age `R²` and `% mediated by batch` by IQM.
    - Row 2: GE age-IQM GAM scatter panels by software version.
    - Row 3: GE FA age effects before vs after QC covariate inclusion.
- Interpretation in main text: many IQM-age relationships are batch-driven; selecting less batch-confounded IQMs avoids distortion of developmental effects.

## Brain tract visualizations (`scripts/6_figures/brain_plots`)

These are supplemental tract-rendered visualizations that map selected effect sizes onto tract geometry.

```{note}
Joelle generated these tract-rendered outputs using a local DSI Studio installation. We currently do not have DSI Studio installed on Respublica, so the lab replicator is not expected to regenerate these renders there.
```

### What is plotted

The tract plotting workflow is designed to visualize effect-size columns from `bundle_statistics.csv`. For the main examples, these are:

- `DKI_mkt` batch effects (from pooled, `source == "raw"`, no-quality batch models in `build_bundle_statistics_csv.R`)
- `NODDI_icvf` age effects (no-quality and/or with contrast, depending on selected column)
- `GQI_fa` quality effects for preprocessed dMRI contrast (`t1post_dwi_contrast`)

In the current `plot_steven_metrics.py`, active plotted columns are controlled by `EFFECT_COLUMNS` and can be toggled there.

The source table is built by:

- Relative to project root: `scripts/6_figures/brain_plots/build_bundle_statistics_csv.R`
- `scripts/6_figures/brain_plots/build_bundle_statistics_csv.R`

which writes:

- `${PROJECT_ROOT}/data/bundle_statistics.csv`

That CSV is then consumed by:

- `scripts/6_figures/brain_plots/plot_steven_metrics.py`
- `scripts/6_figures/brain_plots/tract_visualizer.py`

### How to run

Build the bundle statistics table first:

```bash
Rscript "${PROJECT_ROOT}/scripts/6_figures/brain_plots/build_bundle_statistics_csv.R"
```

Then run the tract plotting script:

```bash
PY_ENV=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["python_env"])' "$CONFIG_PATH")
"${PY_ENV}/bin/python" "${PROJECT_ROOT}/scripts/6_figures/brain_plots/plot_steven_metrics.py"
```
