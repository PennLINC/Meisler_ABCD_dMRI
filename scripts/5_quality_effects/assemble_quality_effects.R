#!/usr/bin/env Rscript

# ============================================================
# ASSEMBLE QUALITY EFFECTS
# ============================================================
# Purpose:
#   Combine quality-effect outputs into one table:
#     1) Pooled: quality_effects_outputs (metric__qc_quality_effects.rds)
#     2) Vendorwise: quality_effects_outputs_vendorwise (metric__qc__VENDOR_quality_effects_by_vendor.rds)
#   Quality effects only (image_quality_metrics, no no_quality). Harmonized data.
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
base_dir <- fs::path(project_root, "data", "quality_effects")
non_vendor_dir <- fs::path(base_dir, "quality_effects_outputs")
vendor_dir <- fs::path(base_dir, "quality_effects_outputs_vendorwise")

if (!fs::dir_exists(non_vendor_dir) && !fs::dir_exists(vendor_dir)) {
  stop(
    "No quality-effect output directories found. Checked:\n",
    " - ", non_vendor_dir, "\n",
    " - ", vendor_dir
  )
}

non_vendor_files <- if (fs::dir_exists(non_vendor_dir)) {
  fs::dir_ls(non_vendor_dir, glob = "*_quality_effects.rds")
} else {
  character(0)
}

vendor_files <- if (fs::dir_exists(vendor_dir)) {
  fs::dir_ls(vendor_dir, glob = "*_quality_effects_by_vendor.rds")
} else {
  character(0)
}

if (length(non_vendor_files) == 0 && length(vendor_files) == 0) {
  stop(
    "No quality-effect RDS files found. Checked:\n",
    " - ", non_vendor_dir, "/*_quality_effects.rds\n",
    " - ", vendor_dir, "/*_quality_effects_by_vendor.rds"
  )
}

log_info("Found non-vendorwise files:", length(non_vendor_files))
log_info("Found vendorwise files:", length(vendor_files))

load_non_vendor_file <- function(f) {
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
  if (!"scanner_manufacturer" %in% names(df)) {
    df <- mutate(df, scanner_manufacturer = "all")
  }
  df <- mutate(df, output_type = "non_vendorwise_pairwise")
  df
}

load_vendor_file <- function(f) {
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
  if (!"scanner_manufacturer" %in% names(df)) {
    df <- mutate(df, scanner_manufacturer = "all")
  }
  df <- mutate(df, output_type = "vendorwise_pairwise")
  df
}

log_info("Loading quality-effect files")
combined_non_vendor <- map_dfr(non_vendor_files, load_non_vendor_file)
combined_vendor <- map_dfr(vendor_files, load_vendor_file)
combined <- bind_rows(combined_non_vendor, combined_vendor)

out_rds <- fs::path(base_dir, "quality_effects_all_outputs.rds")
saveRDS(combined, out_rds)

log_info("Saved combined quality-effect rows:", nrow(combined))
log_info("Saved:", out_rds)
