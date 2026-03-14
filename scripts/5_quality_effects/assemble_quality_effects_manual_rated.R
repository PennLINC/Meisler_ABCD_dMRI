#!/usr/bin/env Rscript

# ============================================================
# ASSEMBLE QUALITY EFFECTS (MANUALLY RATED, BY VENDOR + POOLED)
# ============================================================
# Purpose:
#   Combine per-metric RDS outputs from quality_effects_manual_rated_outputs
#   into a single RDS (with vendor column: GE, Philips, Siemens, pooled).
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(fs)
  library(purrr)
})

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
if (!fs::file_exists(config_path)) {
  stop("CONFIG_PATH does not exist: ", config_path)
}

config <- jsonlite::fromJSON(config_path)
project_root <- normalizePath(config$project_root, mustWork = FALSE)
out_dir <- fs::path(project_root, "data", "quality_effects", "quality_effects_manual_rated_outputs")

if (!fs::dir_exists(out_dir)) {
  stop("Output directory not found: ", out_dir)
}

pattern <- "*_quality_effects_manual_rated.rds"
rds_files <- fs::dir_ls(out_dir, glob = pattern)

if (length(rds_files) == 0) {
  stop("No RDS files matching ", pattern, " in ", out_dir)
}

log_info("Found", length(rds_files), "manual-rated quality effect files")

combined <- map_dfr(rds_files, function(f) {
  df <- readRDS(f)
  if (!is.data.frame(df)) {
    stop("File is not a data.frame/tibble: ", f)
  }
  df
})

out_rds <- fs::path(project_root, "data", "quality_effects", "quality_effects_manual_rated_all_outputs.rds")
saveRDS(combined, out_rds)

log_info("Saved combined manual-rated quality effects: nrow =", nrow(combined))
log_info("Output:", out_rds)
