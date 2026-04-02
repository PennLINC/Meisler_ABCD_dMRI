# Setting up for project replication

We will only be replicating steps that happen after manual rating/classification and data munging. These include the statistical analyses and figure making for what is presented in the main text.


## Project portability via `config.json`

This project is designed to be portable across machines by centralizing machine-specific paths and settings in `config.json` at the project root.

If you are the lab reproducibility, you will be replicating the project on Respublica at `/mnt/isilon/bgdlab_hbcd/projects/meisler_abcd_replication`.
```{note}
I have already made this directory for the lab replicator and updated the `config.json` accordingly.
```

Scripts expect the environment variable `CONFIG_PATH` to point to this file:
```bash
export CONFIG_PATH="/absolute/path/to/your/config.json"
```

To persist this across terminal sessions, add it to your `bashrc`:
```bash
echo 'export CONFIG_PATH="/absolute/path/to/config.json"' >> ~/.bashrc
source ~/.bashrc
```

Update these fields in `config.json` for your system:
- `project_root`: absolute path to this repository
- `lasso_root`: absolute path to your LASSO/ABCD input data directory **(only necessary if re-munging data, which shouldn't be the case given LASSO will be making the QSIRecon data official tabular data)**
- `r_env`: Path to your R envionment (should contain `bin/Rscript`). **Lab replicators can feel free to use mine, otherwise make a new one (see below)**
- `python_env`: Path to your Python envionment (can be same as `r_env`, should contain `bin/python`). **Lab replicators can feel free to use mine, otherwise make a new one (see below). Only necessary if doing model retraining!**

Other fields in `config.json` control shared analysis behavior (for example metric lists and plot style defaults), which helps keep all scripts synchronized. **These do not have to be changed**.

## Software dependencies
A computational environment must have the following packages (and dependencies):
- R:
```
install.packages(c(
  "arrow",
  "ComBatFamily",
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
`remotes::install_github("Nhillman19/ComBatFamily")'

For notebook execution (interactive or headless), also install:
```
pip install jupyterlab notebook nbconvert
```

If you will be rerunning the quality classifier, you will also need the following Python dependencies:
```
pip install ipython joblib matplotlib numpy pandas scikit-learn scikit-optimize scipy seaborn xgboost
```

If you will be generating brain tract visualizations (`scripts/6_figures/brain_plots`), install this full Python set (listed in full even if overlapping with other steps):
```
pip install numpy pandas matplotlib seaborn scipy Pillow openpyxl
```
Notes:
- `pandas` / `numpy` are used for loading and reshaping tract statistics.
- `matplotlib` and `seaborn` are used for colormaps and figure generation.
- `Pillow` is used for image compositing/resizing in `tract_visualizer.py`.
- `openpyxl` is used when tract abbreviations are loaded from `.xlsx`.
- You also need a local DSI Studio executable for final tract rendering (see Figure chapter notes).

Install the R kernel for Jupyter:
```bash
"${R_ENV}/bin/Rscript" -e "IRkernel::installspec(name='r', displayname='R')"
"${PY_ENV}/bin/jupyter" kernelspec list
```
