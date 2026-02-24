#!/usr/bin/env Rscript

# ============================================================
# ASSEMBLE VENDOR EFFECTS
# ============================================================
# Purpose:
#   Combine outputs from calculate_vendor_effects.R into one
#   cached table for plotting/analysis.
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
if (!file_exists(config_path)) {
  stop("CONFIG_PATH does not exist: ", config_path)
}

config <- jsonlite::fromJSON(config_path)
project_root <- normalizePath(config$project_root, mustWork = FALSE)

base_dir <- fs::path(project_root, "data", "vendor_effects")
input_dir <- fs::path(base_dir, "vendor_effects_outputs")

if (!dir_exists(input_dir)) {
  stop("Vendor effects directory not found: ", input_dir)
}

files <- dir_ls(input_dir, glob = "*_vendor_effects.rds")
if (length(files) == 0) {
  stop("No vendor-effect files found in: ", input_dir)
}

log_info("Found vendor-effect files:", length(files))

load_file <- function(f) {
  metric_name <- sub("__.*$", "", basename(f))
  df <- readRDS(f)

  if (!is.data.frame(df)) {
    stop("File is not a data.frame/tibble: ", f)
  }

  if (!"metric" %in% names(df)) {
    df <- mutate(df, metric = metric_name)
  }
  if (!"source" %in% names(df)) {
    df <- mutate(df, source = "harmonized")
  }
  if (!"qc_covariate" %in% names(df)) {
    # Backward-safe default; current script writes this.
    df <- mutate(df, qc_covariate = NA_character_)
  }

  mutate(df, output_type = "vendor_effects")
}

log_info("Loading vendor-effect files")
combined <- map_dfr(files, load_file)

out_rds <- fs::path(base_dir, "vendor_effects_all_outputs.rds")
saveRDS(combined, out_rds)

log_info("Saved combined rows:", nrow(combined))
log_info("Saved:", out_rds)
