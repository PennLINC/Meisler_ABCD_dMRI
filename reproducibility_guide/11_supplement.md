# Supplemental Figures And Analyses

This chapter documents the scripts and notebooks in `scripts/8_supplement` used to generate supplemental figures (S2-S16) and supporting supplementary tables.

```{important}
Many supplemental analyses require the **full modeling run** across all configured microstructural metrics and quality covariates.  
This is different from the efficient main-text subset workflow described in [8_modeling.md](8_modeling.md), which intentionally restricts metrics/arrays for Figures 2-7.
```

## Full-run prerequisites

Before running supplement notebooks, make sure you have completed:

- Harmonized merged data:
  - `data/harmonized_data/merged_data_meisler_analyses_harmonized.parquet`
- Assembled batch effects:
  - `data/batch_effects/batch_effects_all_outputs.rds`
- Assembled age effects (including vendorwise outputs):
  - `data/age_effects/age_effects_all_outputs.rds`
- Assembled quality effects:
  - `data/quality_effects/quality_effects_all_outputs.rds`

In practice, this usually means running the modeling scripts in Chapter 8 without limiting to the main-text subset arrays.

## Running supplement notebooks

Open and run interactively:

```bash
jupyter lab "${PROJECT_ROOT}/scripts/8_supplement"
```

Or execute from CLI:

```bash
cd "${PROJECT_ROOT}/scripts/8_supplement"
for nb in FigureS2_S6.ipynb FigureS7_S9.ipynb FigureS10.ipynb FigureS11.ipynb FigureS12.ipynb FigureS13.ipynb FigureS14.ipynb FigureS15.ipynb FigureS16.ipynb; do
  "${PY_ENV}/bin/jupyter" nbconvert --to notebook --execute --inplace "$nb"
done
```

Outputs are written to `${PROJECT_ROOT}/figures/Supplement/FigureS*/`.

## Notebook-to-figure mapping

- `FigureS2_S6.ipynb`: scanner-manufacturer distributions for IQM families (S2-S6).
- `FigureS7_S9.ipynb`: batch (`batch_device_software`) distributions for selected IQMs (S7-S9).
- `FigureS10.ipynb`: no-quality batch-effect heatmap across all configured microstructural metrics.
- `FigureS11.ipynb`: no-quality pooled harmonized age-effect heatmap across all configured microstructural metrics.
- `FigureS12.ipynb`: cross-vendor correspondence of bundlewise age effects, pre/post harmonization, across all configured microstructural metrics.
- `FigureS13.ipynb`: cross-metric age-effect correlation heatmap across all configured microstructural metrics.
- `FigureS14.ipynb`: winner-take-all maximum quality-effect heatmap across all configured microstructural metrics and bundles.
- `FigureS15.ipynb`: IQM covariance/correlation heatmap from harmonized data.
- `FigureS16.ipynb`: FA/MD software comparison panels for batch, age, and quality effects.

`FigureS14_old.ipynb` and `FigureS15_old.ipynb` are legacy versions retained in the directory and are not part of the current supplemental figure workflow.

## Supplementary tables in `scripts/8_supplement`

- `supplementary_table_S1_IQMs`: Python script that generates an HTML IQM definitions table.
- `supplementary_table_S2_microstructure.html`: static HTML table of microstructural metrics and categories.
- `supplementary_table_S3_batches`: Python script that generates an HTML harmonization-batch table from harmonized data.

