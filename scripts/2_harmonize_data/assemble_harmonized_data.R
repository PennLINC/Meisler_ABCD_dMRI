#!/usr/bin/env Rscript

# ============================================================
# ASSEMBLE HARMONIZED DATA
# ============================================================
# Purpose:
#   Combine per-metric harmonization outputs (harm_*.parquet) into a
#   single dataframe and replace original bundle metric columns with the
#   harmonized versions.
# ============================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fs)
  library(purrr)
  library(stringr)
})

# ============================================================
# HELPERS
# ============================================================

log_info <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n")
}

assert_unique_keys <- function(df, key_cols, source_label) {
  key_counts <- df %>%
    count(across(all_of(key_cols)), name = "n") %>%
    filter(n > 1)
  if (nrow(key_counts) > 0) {
    stop(source_label, " has duplicated keys for ", paste(key_cols, collapse = ", "))
  }
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
metrics <- as.character(config$microstructural_metrics)

if (length(metrics) == 0) {
  stop("microstructural_metrics is missing or empty in config.json")
}

id_col <- "subject_id"
session_col <- "session_id"
key_cols <- c(id_col, session_col)

raw_data_dir <- fs::path(PROJECT_ROOT, "data", "raw_data")
harmonized_dir <- fs::path(PROJECT_ROOT, "data", "harmonized_data", "harmonized_parts")
output_dir <- fs::path(PROJECT_ROOT, "data", "harmonized_data")
dir_create(output_dir, recurse = TRUE)

base_input_file <- fs::path(raw_data_dir, "merged_data_meisler_analyses.parquet")
output_file <- fs::path(output_dir, "merged_data_meisler_analyses_harmonized.parquet")

if (!file_exists(base_input_file)) {
  stop("Base input parquet does not exist: ", base_input_file)
}
if (!dir_exists(harmonized_dir)) {
  stop("Harmonized outputs directory does not exist: ", harmonized_dir)
}

# ============================================================
# LOAD BASE DATA
# ============================================================

log_info("Loading base dataset:", base_input_file)
base_df <- read_parquet(base_input_file)
assert_unique_keys(base_df, key_cols, "Base dataset")
log_info("Base rows:", nrow(base_df), "cols:", ncol(base_df))

# ============================================================
# LOAD HARMONIZED METRIC FILES
# ============================================================

expected_files <- fs::path(harmonized_dir, paste0("harm_", metrics, ".parquet"))
available_files <- expected_files[file_exists(expected_files)]
missing_metrics <- metrics[!file_exists(expected_files)]

if (length(available_files) == 0) {
  stop("No harmonized files found in: ", harmonized_dir)
}

if (length(missing_metrics) > 0) {
  log_info(
    "Warning: missing harmonized files for",
    length(missing_metrics), "metrics"
  )
  log_info("Missing metrics:", str_c(missing_metrics, collapse = ", "))
}

load_harmonized_file <- function(f) {
  metric_name <- str_remove(path_ext_remove(path_file(f)), "^harm_")
  log_info("Loading harmonized metric:", metric_name)

  df <- read_parquet(f)
  if (!all(key_cols %in% names(df))) {
    stop("Missing key columns in harmonized file: ", f)
  }
  assert_unique_keys(df, key_cols, paste0("Harmonized file ", path_file(f)))

  harm_cols <- names(df)[str_ends(names(df), "_harmonized")]
  if (length(harm_cols) == 0) {
    stop("No *_harmonized columns found in: ", f)
  }

  df %>% select(all_of(key_cols), all_of(harm_cols))
}

harmonized_dfs <- map(available_files, load_harmonized_file)
harmonized_wide <- reduce(harmonized_dfs, full_join, by = key_cols)
log_info("Combined harmonized rows:", nrow(harmonized_wide), "cols:", ncol(harmonized_wide))

# ============================================================
# KEEP ONLY HARMONIZED BUNDLE STATS (DROP RAW BUNDLE COLUMNS)
# ============================================================

harm_cols_all <- names(harmonized_wide)[str_ends(names(harmonized_wide), "_harmonized")]
if (length(harm_cols_all) == 0) {
  stop("No harmonized columns found after join.")
}

# Rename harmonized columns back to base names (remove _harmonized suffix).
harmonized_clean <- harmonized_wide %>%
  rename_with(~ str_remove(.x, "_harmonized$"), .cols = all_of(harm_cols_all))

# Drop all original bundle stats (raw mean/median/etc.) from base dataframe.
raw_bundle_cols <- names(base_df)[str_starts(names(base_df), "bundle_")]

merged_df <- base_df %>%
  select(-any_of(raw_bundle_cols)) %>%
  left_join(harmonized_clean, by = key_cols)

log_info("Dropped raw bundle columns from base:", length(raw_bundle_cols))
log_info("Added harmonized bundle columns:", length(harm_cols_all))

log_info("Final rows:", nrow(merged_df), "cols:", ncol(merged_df))

# ============================================================
# SAVE
# ============================================================

write_parquet(merged_df, output_file)
log_info("Saved harmonized merged dataset:", output_file)
