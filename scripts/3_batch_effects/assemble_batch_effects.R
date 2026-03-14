#!/usr/bin/env Rscript

# ============================================================
# ASSEMBLE BATCH EFFECTS
# ============================================================
# Purpose:
#   Combine outputs from calculate_batch_effects.R (pooled) and
#   calculate_batch_effects_vendorwise.R into one cached table.
#   Pooled:   output_type == "batch_effects"
#   Vendorwise: output_type == "batch_effects_vendorwise" (rows include
#   scanner_manufacturer = GE | Philips | Siemens)
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

base_dir <- fs::path(project_root, "data", "batch_effects")
input_dir_pooled <- fs::path(base_dir, "batch_effects_outputs")
input_dir_vendorwise <- fs::path(base_dir, "batch_effects_outputs_vendorwise")

load_pooled <- function(f) {
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
    df <- mutate(df, qc_covariate = NA_character_)
  }

  mutate(df, output_type = "batch_effects")
}

load_vendorwise <- function(f) {
  df <- readRDS(f)

  if (!is.data.frame(df)) {
    stop("File is not a data.frame/tibble: ", f)
  }

  if (!"metric" %in% names(df)) {
    # Filename: metric__qc__Vendor_batch_effects_by_vendor.rds
    metric_name <- sub("__.*$", "", basename(f))
    df <- mutate(df, metric = metric_name)
  }
  if (!"source" %in% names(df)) {
    df <- mutate(df, source = "harmonized")
  }
  if (!"qc_covariate" %in% names(df)) {
    df <- mutate(df, qc_covariate = NA_character_)
  }

  mutate(df, output_type = "batch_effects_vendorwise")
}

parts <- list()

if (dir_exists(input_dir_pooled)) {
  files_pooled <- dir_ls(input_dir_pooled, glob = "*_batch_effects.rds")
  if (length(files_pooled) > 0) {
    log_info("Found pooled batch-effect files:", length(files_pooled))
    parts$pooled <- map_dfr(files_pooled, load_pooled)
  } else {
    log_info("No pooled *_batch_effects.rds in:", input_dir_pooled)
  }
} else {
  log_info("Pooled directory not found (skipping):", input_dir_pooled)
}

if (dir_exists(input_dir_vendorwise)) {
  files_vw <- dir_ls(input_dir_vendorwise, glob = "*_batch_effects_by_vendor.rds")
  if (length(files_vw) > 0) {
    log_info("Found vendorwise batch-effect files:", length(files_vw))
    parts$vendorwise <- map_dfr(files_vw, load_vendorwise)
  } else {
    log_info("No vendorwise *_batch_effects_by_vendor.rds in:", input_dir_vendorwise)
  }
} else {
  log_info("Vendorwise directory not found (skipping):", input_dir_vendorwise)
}

if (length(parts) == 0) {
  stop(
    "No batch-effect RDS found. Run calculate_batch_effects.R and/or ",
    "calculate_batch_effects_vendorwise.R, then re-run assemble."
  )
}

combined <- bind_rows(parts)

out_rds <- fs::path(base_dir, "batch_effects_all_outputs.rds")
saveRDS(combined, out_rds)

log_info("Saved combined rows:", nrow(combined))
log_info("By output_type:", paste(unique(combined$output_type), collapse = ", "))
log_info("Saved:", out_rds)
