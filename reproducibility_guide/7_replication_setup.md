# Setting up for project replication

We will only be replicating steps that happen after manual rating/classification and data munging. These include the statistical analyses and figure making for what is presented in the main text.

## Project portability via `config.json`

This project is designed to be portable across machines by centralizing machine-specific paths and settings in `config.json` at the project root.

If you are the lab reproducibility, you will be replicating the project on Respublica at `/mnt/isilon/bgdlab_hbcd/projects/meisler_abcd_replication`.

```{note}
I have already made this directory for the lab replicator and provided an `config.json` accordingly.
```

### Clone the repository:

In `/mnt/isilon/bgdlab_hbcd/projects/meisler_abcd_replication`, with a GitHub SSH key enabled, run:

```
git clone git@github.com:PennLINC/Meisler_ABCD_dMRI.git
```

You will see there is an example `config.json` in the cloned repository. You can copy the one I made for you to overwrite it. Regardless, scripts expect the environment variable `CONFIG_PATH` to point to a proper config file:

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
```

To persist this across terminal sessions, add it to your `bashrc`:

```bash
echo 'export CONFIG_PATH="/absolute/path/to/config.json"' >> ~/.bashrc
source ~/.bashrc
```

Update these fields in `config.json` as necessary for your system:

- `project_root`: absolute path to this repository
- `lasso_root`: absolute path to your LASSO/ABCD input data directory **(only necessary if re-munging data, which shouldn't be the case given LASSO will be making the QSIRecon data official tabular data)**
- `r_env`: Path to your R envionment (should contain `bin/Rscript`). **Lab replicators can feel free to use mine, otherwise make a new one (see below)**
- `python_env`: Path to your Python envionment (can be same as `r_env`, should contain `bin/python`). **Lab replicators can feel free to use mine, otherwise make a new one (see below). Only necessary if doing model retraining or tract plot visualization!**

Other fields in `config.json` control shared analysis behavior (for example metric lists and plot style defaults), which helps keep all scripts synchronized. **These do not have to be changed**.

## Copy pre-built analysis tables (lab replicator)

Because the lab replicator is not expected to re-munge the raw ABCC inputs, you can copy the prepared analysis parquet files directly:

```bash
mkdir -p "${PROJECT_ROOT}/data/raw_data/"
cp /mnt/isilon/bgdlab_hbcd/projects/meisler_abcd_dmri_new/data/raw_data/merged_data_meisler_analyses.parquet "${PROJECT_ROOT}/data/raw_data/"
```

This provides the raw munged data, filtered for exclusion criteria, in ready-to-harmonize form.

## Quality classifier workflow

Quality-classifier retraining/deployment details are documented in the dedicated classifier chapter (immediately after manual rating), not in setup.

## SLURM logs

For guide steps that use `sbatch`, job stdout/stderr logs are written to the `logs/` folder inside the corresponding script directory (for example, `scripts/2_harmonize_data/logs`, `scripts/3_batch_effects/logs`).

## Software dependencies

A computational environment must have the following packages (and dependencies):

- R:

```
install.packages(c(
  "arrow",
  "dplyr",
  "fs",
  "gamm4",
  "ggplot2",
  "IRkernel",
  "jsonlite",
  "lme4",
  "mgcv",
  "purrr",
  "readr",
  "remotes",
  "stringr",
  "systemfonts",
  "tibble",
  "tidyr"
))
```

and the harmonization requires a certain branch of CombatFamily:  
`remotes::install_github("Nhillman19/ComBatFamily")`

For notebook execution (interactive or headless), also install:

```
pip install jupyterlab notebook nbconvert
```

If you will be rerunning the quality classifier, you will also need the following Python dependencies:

```
pip install ipython joblib matplotlib numpy pandas scikit-learn scikit-optimize scipy seaborn shap xgboost
```

If you will be generating brain tract visualizations (`scripts/6_figures/brain_plots`), install the following packages:

```
pip install numpy pandas matplotlib seaborn scipy Pillow openpyxl
```

```{node}

Note you also need a local DSI Studio executable if you want to create  tract rendering (see Figure chapter notes). The lab replicator is not expected to do this.

```

### Using Jupyter for figures

The repository uses `.ipynb` files for interactive analyses.

Install the R kernel for Jupyter:

```bash
"${R_ENV}/bin/Rscript" -e "IRkernel::installspec(name='r', displayname='R')"
"${PY_ENV}/bin/jupyter" kernelspec list
```

Figure/supplement notebooks first check for the environmental`CONFIG_PATH` variable, and if it is unset or cannot be found they now fall back to searching upward from the current working directory for `config.json`.

To make that fallback reliable, launch Jupyter from the project root:

```bash
cd "${PROJECT_ROOT}"
"${PY_ENV}/bin/jupyter" lab
```

