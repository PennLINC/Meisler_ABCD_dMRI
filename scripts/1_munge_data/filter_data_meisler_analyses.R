#!/usr/bin/env Rscript

# ============================================================
# FILTER DATA FOR MEISLER ANALYSES
# ============================================================
# Purpose:
#   Filter merged dMRI data to rows that are:
#   1) not excluded (`not_excluded == TRUE`), and
#   2) complete (no missing values) across required bundle columns.
#   3) define harmonization batch column (batch_device_software)
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(fs)
  library(stringr)
})

# ============================================================
# CONFIGURATION
# ============================================================

log_info <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n")
}

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

raw_data_dir <- fs::path(PROJECT_ROOT, "data", "raw_data")
dir_create(raw_data_dir, recurse = TRUE)

input_path <- fs::path(raw_data_dir, "merged_data.parquet")
output_path <- fs::path(raw_data_dir, "merged_data_meisler_analyses.parquet")

if (!file_exists(input_path)) {
  stop("Input parquet does not exist: ", input_path)
}

# ============================================================
# LOAD DATA
# ============================================================

log_info("Loading data:", input_path)
df <- read_parquet(input_path)
log_info("Data loaded. Rows:", nrow(df), "Cols:", ncol(df))

# ============================================================
# DEFINE REQUIRED BUNDLE COLUMNS
# ============================================================

ignored_bundle_prefixes <- c(
  "bundle_ProjectionBrainstem_DentatorubrothalamicTract-lr",
  "bundle_ProjectionBrainstem_DentatorubrothalamicTract-rl",
  "bundle_Commissure_AnteriorCommissure",
  "bundle_ProjectionBrainstem_CorticobulbarTractL",
  "bundle_ProjectionBrainstem_CorticobulbarTractR"
)

all_bundle_cols <- names(df)[str_starts(names(df), "bundle_")]

# Bundle columns to remove entirely from downstream datasets.
ignored_bundle_cols <- all_bundle_cols[vapply(
  all_bundle_cols,
  function(col_name) any(startsWith(col_name, ignored_bundle_prefixes)),
  logical(1)
)]

# Keep only bundle columns that do not match ignored bundle prefixes.
required_bundle_cols <- all_bundle_cols[!vapply(
  all_bundle_cols,
  function(col_name) any(startsWith(col_name, ignored_bundle_prefixes)),
  logical(1)
)]

log_info("Total bundle_ columns:", length(all_bundle_cols))
log_info("Ignored bundle prefixes:", length(ignored_bundle_prefixes))
log_info("Ignored bundle columns to drop:", length(ignored_bundle_cols))
log_info("Required bundle columns:", length(required_bundle_cols))

# ============================================================
# FILTER DATA
# ============================================================

if (!"not_excluded" %in% names(df)) {
  stop("Column 'not_excluded' not found in input dataframe.")
}

if (length(required_bundle_cols) == 0) {
  stop("No required bundle columns found after filtering ignored bundles.")
}

filtered_df <- df %>%
  filter(
    not_excluded,
    rowSums(is.na(select(., all_of(required_bundle_cols)))) == 0
  ) %>%
  select(-any_of(ignored_bundle_cols))

log_info("Ignored bundle columns dropped from output:", length(ignored_bundle_cols))
log_info("Output columns after drop:", ncol(filtered_df))

log_info("Rows after filtering (not_excluded + complete bundles):", nrow(filtered_df))

# ============================================================
# DEFINE MAJOR SOFTWARE + BATCH (Device × Major Software)
# ============================================================

if (!all(c("DeviceSerialNumber", "scanner_software") %in% names(filtered_df))) {
  stop("DeviceSerialNumber and/or scanner_software not found in dataframe.")
}

filtered_df <- filtered_df %>%
  mutate(
    software_major = case_when(
      
      # ---- GE ----
      str_detect(scanner_software, "DV25") ~ "GE_DV25",
      str_detect(scanner_software, "DV26") ~ "GE_DV26",
      str_detect(scanner_software, "DV29") ~ "GE_DV29",
      str_detect(scanner_software, "RX28") ~ "GE_RX28",
      
      # ---- Siemens ----
      str_detect(scanner_software, "VE11B") ~ "Siemens_VE11B",
      str_detect(scanner_software, "VE11C") ~ "Siemens_VE11C",
      str_detect(scanner_software, "VE11E") ~ "Siemens_VE11E",
      
      # ---- Philips ----
      str_detect(scanner_software, "5\\.3") ~ "Philips_5.3",
      str_detect(scanner_software, "5\\.4") ~ "Philips_5.4",
      str_detect(scanner_software, "5\\.6") ~ "Philips_5.6",
      str_detect(scanner_software, "5\\.7") ~ "Philips_5.7",
      
      TRUE ~ "Other"
    ),
    
    # ---- FINAL BATCH COLUMN ----
    batch_device_software = paste(DeviceSerialNumber, software_major, sep = ".")
  )

# ============================================================
# REMOVE SMALL BATCHES (< 10 SESSIONS)
# ============================================================

batch_counts <- filtered_df %>%
  group_by(batch_device_software) %>%
  summarise(n_sessions = n(), .groups = "drop")

valid_batches <- batch_counts %>%
  filter(n_sessions >= 10) %>%
  pull(batch_device_software)

filtered_df <- filtered_df %>%
  filter(batch_device_software %in% valid_batches)

log_info("Batches before size filter:", nrow(batch_counts))
log_info("Batches retained (>=10 sessions):", length(valid_batches))
log_info("Rows after removing small batches:", nrow(filtered_df))

# ============================================================
# SAVE OUTPUT
# ============================================================

write_parquet(filtered_df, output_path)
log_info("Saved filtered dataset:", output_path)

