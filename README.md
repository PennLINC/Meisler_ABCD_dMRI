# Meisler et al. ABCC dMRI Project

Code, analysis workflows, and reproducibility documentation for the Meisler et al. ABCC diffusion MRI paper.

This repository covers:

- dMRI post-processing and tabular data assembly
- harmonization across scanner/device-software batches
- modeling of batch, age, and quality effects
- main-text and supplemental figure generation
- notebook-based manuscript results summaries

## Start Here

The fastest way to get oriented is the reproducibility guide:

- [Reproducibility Guide index](reproducibility_guide/0_basic_info.md)
- [Replication setup](reproducibility_guide/6_replication_setup.md)
- [Modeling workflow](reproducibility_guide/8_modeling.md)
- [Main figures](reproducibility_guide/9_figures.md)
- [Supplemental figures](reproducibility_guide/11_supplement.md)

If you are replicating the paper analyses (rather than reprocessing raw imaging), begin at Chapter 6 and proceed in order.

## Repository Structure

```text
.
├── config.json                  # Machine-specific paths + shared analysis settings
├── data/                        # Derived tabular data inputs/outputs
├── figures/                     # Generated figure outputs
├── reproducibility_guide/       # Jupyter Book source for full methods/replication docs
└── scripts/
    ├── 0_processing/            # QSIPrep/QSIRecon processing scripts and configs (not to be replicated)
    ├── 1_munge_data/            # Build and filter merged analysis table
    ├── 2_harmonize_data/        # Longitudinal ComBat-GAM harmonization
    ├── 3_batch_effects/         # Batch effects modeling
    ├── 4_age_effects/           # Age effects modeling (pooled + vendorwise)
    ├── 5_quality_effects/       # IQM/manual quality effects modeling
    ├── 6_figures/               # Main-text figure notebooks + tract plotting scripts
    ├── 7_results/               # Notebook-generated manuscript/table statistics
    └── 8_supplement/            # Supplemental figure notebooks and table scripts
```

## Quick Setup

1. Set up `config.json` for your machine (especially `project_root`, `r_env`, `python_env`).
2. Export `CONFIG_PATH` so scripts can find your config.
3. Install required R/Python packages (see [Replication setup](reproducibility_guide/6_replication_setup.md)).

```bash
export CONFIG_PATH="/absolute/path/to/this/repo/config.json"
```

## Analysis Flow

1. Data munging/filtering (`scripts/1_munge_data`)
2. Harmonization (`scripts/2_harmonize_data`)
3. Modeling (`scripts/3_batch_effects`, `scripts/4_age_effects`, `scripts/5_quality_effects`)
4. Main figures/results (`scripts/6_figures`, `scripts/7_results`)
5. Supplemental analyses (`scripts/8_supplement`)

Detailed commands and assumptions are documented in the reproducibility guide chapters.

Note some steps are PennLINC-internal or depend on ABCC/ABCD controlled-access infrastructure.