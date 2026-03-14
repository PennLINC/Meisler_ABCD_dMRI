#!/usr/bin/env Rscript

# ============================================================
# HARMONIZE DATA (SLURM ARRAY: ONE FEATURE PER TASK)
# ============================================================
# Purpose:
#   Run longitudinal ComBat-GAM harmonization for one feature suffix
#   selected by SLURM array ID, across bundle median columns
# ============================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fs)
  library(mgcv)
  library(ComBatFamily)
  library(gamm4)
})

# ============================================================
# HELPERS
# ============================================================

log_info <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n")
}

get_script_dir <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- "--file="
  file_match <- grep(file_arg, args)
  if (length(file_match) > 0) {
    script_path <- sub(file_arg, "", args[file_match[1]])
    return(dirname(normalizePath(script_path, mustWork = FALSE)))
  }
  normalizePath(getwd(), mustWork = FALSE)
}

# ============================================================
# CONFIGURATION
# ============================================================

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("jsonlite package is required. Install with: install.packages('jsonlite')")
}

config_path <- Sys.getenv("CONFIG_PATH", unset = "")
if (!nzchar(config_path)) {
  stop("CONFIG_PATH is not set. Export CONFIG_PATH=/path/to/config.json before running.")
}

config_path <- normalizePath(config_path, mustWork = FALSE)
if (!file_exists(config_path)) {
  stop("CONFIG_PATH does not exist: ", config_path)
}

config <- jsonlite::fromJSON(config_path)
PROJECT_ROOT <- normalizePath(config$project_root, mustWork = FALSE)

script_dir <- get_script_dir()
comfam_path <- fs::path(script_dir, "comfam.R")
if (!file_exists(comfam_path)) {
  stop("Required file not found: ", comfam_path)
}
source(comfam_path)

raw_data_dir <- fs::path(PROJECT_ROOT, "data", "raw_data")
harmonization_output_dir <- fs::path(PROJECT_ROOT, "data", "harmonized_data", "harmonized_parts")
dir_create(harmonization_output_dir, recurse = TRUE)

input_parquet <- fs::path(raw_data_dir, "merged_data_meisler_analyses.parquet")
if (!file_exists(input_parquet)) {
  stop("Input parquet does not exist: ", input_parquet)
}

# Model constants
batch_col <- "batch_device_software"
age_col <- "age"
sex_col <- "sex"
id_col <- "subject_id"
session_col <- "session_id"
k_age <- 4

# Metric suffixes are configured centrally in config.json.
metrics <- config$microstructural_metrics
if (is.null(metrics) || length(metrics) == 0) {
  stop("microstructural_metrics is missing or empty in config.json")
}
metrics <- as.character(metrics)

# Bundles to exclude from harmonization targets
drop_bundle_prefixes <- c(
  # Current naming format
  "bundle_ProjectionBrainstem_DentatorubrothalamicTract-lr",
  "bundle_ProjectionBrainstem_DentatorubrothalamicTract-rl",
  "bundle_Commissure_AnteriorCommissure",
  "bundle_ProjectionBrainstem_CorticobulbarTractL",
  "bundle_ProjectionBrainstem_CorticobulbarTractR"
)

# ============================================================
# SLURM ARRAY TASK
# ============================================================

task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID", "1"))
log_info("Running SLURM array task:", task_id)

if (task_id < 1 || task_id > length(metrics)) {
  stop(
    "Invalid SLURM_ARRAY_TASK_ID: ", task_id,
    ". Must be between 1 and ", length(metrics)
  )
}

feature_suffix <- metrics[task_id]
log_info("Selected feature suffix:", feature_suffix)

# ============================================================
# LOAD + CLEAN
# ============================================================

log_info("Loading:", input_parquet)
df <- read_parquet(input_parquet) %>%
  mutate(
    !!batch_col := as.factor(.data[[batch_col]]),
    !!sex_col   := suppressWarnings(as.numeric(.data[[sex_col]])),
    !!id_col    := as.factor(.data[[id_col]]),
    !!age_col   := suppressWarnings(as.numeric(.data[[age_col]]))
  ) %>%
  filter(
    !is.na(.data[[batch_col]]),
    !is.na(.data[[age_col]]),
    !is.na(.data[[sex_col]]),
    !is.na(.data[[id_col]])
  )
log_info("Rows after required covariate filtering:", nrow(df))

# ============================================================
# COLLECT COLUMNS FOR THIS TASK
# ============================================================

# Bundle drop pattern used when selecting columns.
drop_pattern <- paste0("^(", paste(drop_bundle_prefixes, collapse = "|"), ")_")

bundle_cols <- grep(
  paste0("^bundle_.*_", feature_suffix, "_median$"),
  names(df),
  value = TRUE
)
bundle_cols <- bundle_cols[!grepl(drop_pattern, bundle_cols)]
target_cols <- bundle_cols
if (length(target_cols) == 0L) {
  stop("No columns found for feature suffix: ", feature_suffix)
}

log_info(
  "Columns selected for", feature_suffix, ":",
  length(bundle_cols), "bundles"
)

# ============================================================
# PREP HARMONIZATION INPUTS
# ============================================================

raw_cols <- paste0(target_cols, "_raw")
names(df)[match(target_cols, names(df))] <- raw_cols

gam_formula <- as.formula(
  paste0("y ~ s(", age_col, ", k = ", k_age, ") + ", sex_col)
)

batch_vector <- droplevels(df[[batch_col]])
covariates <- df[, c(age_col, sex_col, id_col), drop = FALSE]

df[raw_cols] <- lapply(df[raw_cols], function(x) suppressWarnings(as.numeric(as.character(x))))
X <- as.data.frame(df[raw_cols])

if (any(sapply(X, function(x) all(is.na(x))))) {
  stop("Feature ", feature_suffix, " contains all-NA columns.")
}

# Remove rows with missing values required for harmonization.
is_bad_row <- !complete.cases(X) | !complete.cases(covariates) | is.na(batch_vector)
log_info("Incomplete rows dropped:", sum(is_bad_row))

X_complete <- X[!is_bad_row, , drop = FALSE]
cov_complete <- covariates[!is_bad_row, , drop = FALSE]
batch_complete <- droplevels(batch_vector[!is_bad_row])

log_info("Harmonizing", length(raw_cols), "columns for feature:", feature_suffix)

# ============================================================
# RUN COMBAT-GAM
# ============================================================

fit <- comfam(
  data = X_complete,
  bat = batch_complete,
  covar = cov_complete,
  model = "gamm4",
  formula = gam_formula,
  random = ~(1 + age | subject_id),
  eb = TRUE,
  verbose = TRUE
)

# ============================================================
# REINSERT + SAVE
# ============================================================

harmonized <- as.data.frame(fit$dat.combat)
names(harmonized) <- sub("_raw$", "_harmonized", names(harmonized))

harmonized_full <- as.data.frame(matrix(NaN, nrow = nrow(df), ncol = ncol(harmonized)))
colnames(harmonized_full) <- names(harmonized)
harmonized_full[!is_bad_row, ] <- harmonized

output_file <- fs::path(harmonization_output_dir, paste0("harm_", feature_suffix, ".parquet"))

out_df <- df %>%
  select(all_of(c(id_col, session_col))) %>%
  bind_cols(harmonized_full)

write_parquet(out_df, output_file)
log_info("Saved:", output_file)
log_info("Done.")

