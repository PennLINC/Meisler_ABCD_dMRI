#!/usr/bin/env Rscript

# ============================================================
# ASSEMBLE AGE EFFECTS
# ============================================================
# Purpose:
#   Combine age-effect outputs into one table:
#     1) Pooled: age_effects_outputs/*__*_age_effects.rds (metric__qc_metric_age_effects.rds) — harmonized only
#     2) Vendorwise: age_effects_outputs_vendorwise/*_age_effects_by_vendor.rds — raw + harmonized per vendor
#   Each RDS contains metric, qc_metric, and effect columns. Adds output_type, source, scanner_manufacturer.
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
base_dir <- fs::path(project_root, "data", "age_effects")
pooled_dir <- fs::path(base_dir, "age_effects_outputs")
vendor_dir <- fs::path(base_dir, "age_effects_outputs_vendorwise")

if (!fs::dir_exists(pooled_dir) && !fs::dir_exists(vendor_dir)) {
  stop(
    "No age-effect output directories found. Checked:\n",
    " - ", pooled_dir, "\n",
    " - ", vendor_dir
  )
}

# Pooled: metric__qc_safe_age_effects.rds (exclude vendorwise by-vendor files)
pooled_files <- if (fs::dir_exists(pooled_dir)) {
  all_pooled <- fs::dir_ls(pooled_dir, glob = "*_age_effects.rds")
  all_pooled[!grepl("_age_effects_by_vendor\\.rds$", basename(all_pooled))]
} else {
  character(0)
}

vendor_files <- if (fs::dir_exists(vendor_dir)) {
  fs::dir_ls(vendor_dir, glob = "*_age_effects_by_vendor.rds")
} else {
  character(0)
}

if (length(pooled_files) == 0 && length(vendor_files) == 0) {
  stop(
    "No age-effect RDS files found. Checked:\n",
    " - ", pooled_dir, "/*__*_age_effects.rds\n",
    " - ", vendor_dir, "/*_age_effects_by_vendor.rds"
  )
}

log_info("Found pooled files:", length(pooled_files))
log_info("Found vendorwise files:", length(vendor_files))

load_pooled <- function(f) {
  df <- readRDS(f)
  if (!is.data.frame(df)) {
    stop("File is not a data.frame/tibble: ", f)
  }
  if (!"metric" %in% names(df)) {
    # Fallback: parse metric from filename (metric__qc_safe_age_effects.rds)
    stub <- sub("_age_effects\\.rds$", "", basename(f))
    metric_name <- sub("__.*$", "", stub)
    df <- mutate(df, metric = metric_name)
  }
  if (!"qc_metric" %in% names(df)) {
    df <- mutate(df, qc_metric = NA_character_)
  }
  df %>%
    mutate(
      output_type = "pooled",
      scanner_manufacturer = "all",
      source = "harmonized"
    )
}

load_vendorwise <- function(f) {
  df <- readRDS(f)
  if (!is.data.frame(df)) {
    stop("File is not a data.frame/tibble: ", f)
  }
  if (!"metric" %in% names(df)) {
    stub <- sub("__.*_age_effects_by_vendor\\.rds$", "", basename(f))
    df <- mutate(df, metric = stub)
  }
  if (!"qc_metric" %in% names(df)) {
    df <- mutate(df, qc_metric = NA_character_)
  }
  df %>%
    mutate(output_type = "vendorwise")
}

log_info("Loading pooled age-effect files")
combined_pooled <- if (length(pooled_files) > 0) {
  map_dfr(pooled_files, load_pooled)
} else {
  tibble()
}

log_info("Loading vendorwise age-effect files")
combined_vendor <- if (length(vendor_files) > 0) {
  map_dfr(vendor_files, load_vendorwise)
} else {
  tibble()
}

combined <- bind_rows(combined_pooled, combined_vendor)

out_rds <- fs::path(base_dir, "age_effects_all_outputs.rds")
saveRDS(combined, out_rds)

log_info("Saved combined age effects rows:", nrow(combined))
log_info("Saved:", out_rds)
