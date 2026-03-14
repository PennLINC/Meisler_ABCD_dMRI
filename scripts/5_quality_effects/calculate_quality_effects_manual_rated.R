#!/usr/bin/env Rscript

# ============================================================
# CALCULATE QUALITY EFFECTS (MANUALLY RATED, BY VENDOR + POOLED)
# ============================================================
# Purpose:
#   Subset to rows with manually_rated == TRUE. Run for each vendor (GE, Philips, Siemens)
#   and pooled (all vendors). No longitudinal clustering; fit GAM (not GAMM). For each
#   bundle x metric x vendor:
#     - Full (t1post_dwi_contrast): value ~ s(age, k=4) + sex + s(t1post_dwi_contrast, k=4) + batch_device_software
#     - Full (mean_rating):         value ~ s(age, k=4) + sex + s(mean_rating, k=4) + batch_device_software
#     - Reduced:                    value ~ s(age, k=4) + sex + batch_device_software (no quality covariate)
#   Quality effect = change in adjusted R² (full - reduced). One job per metric; output has vendor column.
# ============================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(stringr)
  library(tibble)
  library(fs)
  library(mgcv)
})

log_info <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n")
}

safe_numeric <- function(x) {
  x_num <- suppressWarnings(as.numeric(x))
  x_num[is.nan(x_num)] <- NA_real_
  x_num
}

#' Adjusted R² for GAM: 1 - (1 - R²) * (n - 1) / (n - edf - 1)
adj_r2 <- function(gam_summary, n) {
  r2 <- gam_summary$r.sq
  edf <- tryCatch(sum(gam_summary$edf), error = function(e) NA_real_)
  if (is.na(edf) || edf >= n - 1) return(r2)
  1 - (1 - r2) * (n - 1) / (n - edf - 1)
}

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  metric <- NULL
  i <- 1
  while (i <= length(args)) {
    if (args[i] == "--metric" && i + 1 <= length(args)) {
      metric <- args[i + 1]
      i <- i + 2
    } else {
      i <- i + 1
    }
  }
  if (!nzchar(metric)) {
    stop("Usage: calculate_quality_effects_manual_rated.R --metric <metric>")
  }
  list(metric = metric)
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
metrics <- trimws(as.character(config$microstructural_metrics))

task <- parse_args()
metric <- trimws(task$metric)

if (!metric %in% metrics) {
  stop("Metric not in config microstructural_metrics: ", metric)
}

log_info("Running manual-rated quality effects for metric:", metric, "(GE, Philips, Siemens, pooled)")

harmonized_file <- fs::path(PROJECT_ROOT, "data", "harmonized_data", "merged_data_meisler_analyses_harmonized.parquet")
out_dir <- fs::path(PROJECT_ROOT, "data", "quality_effects", "quality_effects_manual_rated_outputs")
dir_create(out_dir, recurse = TRUE)

if (!file_exists(harmonized_file)) {
  stop("Assembled harmonized parquet not found: ", harmonized_file)
}

out_file <- fs::path(out_dir, paste0(metric, "_quality_effects_manual_rated.rds"))
if (file_exists(out_file)) {
  log_info("Output already exists, skipping:", out_file)
  quit(status = 0)
}

required_covars <- c("subject_id", "sex", "age", "scanner_manufacturer", "manually_rated", "batch_device_software", "t1post_dwi_contrast", "mean_rating")
df <- read_parquet(harmonized_file)

for (col in c("scanner_manufacturer", "manually_rated", "batch_device_software", "t1post_dwi_contrast", "mean_rating")) {
  if (!col %in% names(df)) {
    stop("Required column not found in harmonized parquet: ", col)
  }
}

# Subset: manually rated only (manually_rated: TRUE, 1, or "TRUE"/"1")
is_manual <- function(x) {
  if (is.logical(x)) return(x %in% TRUE)
  if (is.numeric(x)) return(!is.na(x) & x != 0)
  as.character(x) %in% c("TRUE", "true", "1", "yes")
}
df_manual <- df %>%
  filter(is_manual(manually_rated))
log_info("Rows after filter (manually_rated):", nrow(df_manual))

bundle_cols <- grep(
  paste0("^bundle_.+_", metric, "_median$"),
  names(df_manual),
  value = TRUE
)
if (length(bundle_cols) == 0) {
  log_info("No harmonized bundle columns found for", metric, "- exiting.")
  quit(status = 0)
}

vendors <- c("GE", "Philips", "Siemens", "pooled")
results <- list()

for (vendor in vendors) {
  if (vendor == "pooled") {
    df_v <- df_manual
  } else {
    df_v <- df_manual %>% filter(as.character(scanner_manufacturer) == vendor)
  }
  if (nrow(df_v) < 50) {
    log_info("  Skipping vendor", vendor, "(n =", nrow(df_v), "< 50)")
    next
  }
  log_info("  Vendor:", vendor, "n =", nrow(df_v))

  for (bundle_col in sort(bundle_cols)) {
    full_bundle <- str_replace(bundle_col, paste0("^bundle_(.+)_", metric, "_median$"), "\\1")
    bundle_category <- str_extract(full_bundle, "^[^_]+")
    bundle <- str_replace(full_bundle, "^[^_]+_", "")

    this_dat <- df_v %>%
    select(
      all_of(required_covars),
      value = !!sym(bundle_col)
    ) %>%
    mutate(
      age = safe_numeric(age),
      value = safe_numeric(value),
      t1post_dwi_contrast = safe_numeric(t1post_dwi_contrast),
      mean_rating = safe_numeric(mean_rating),
      subject_id = factor(subject_id),
      sex = factor(sex),
      batch_device_software = factor(batch_device_software)
    ) %>%
    filter(
      !is.na(value), !is.na(age), !is.na(sex), !is.na(batch_device_software),
      !is.na(t1post_dwi_contrast), !is.na(mean_rating)
    )

  if (nrow(this_dat) < 50) {
    next
  }

  n_obs <- nrow(this_dat)

  # Reduced: no quality covariate (GAM, no random effects); includes batch
  red_mod <- tryCatch(
    gam(value ~ s(age, k = 4) + sex + batch_device_software, data = this_dat),
    error = function(e) NULL
  )
  if (is.null(red_mod)) {
    next
  }
  gam_summary_red <- summary(red_mod)
  r2_adj_red <- adj_r2(gam_summary_red, n_obs)

  # Full with t1post_dwi_contrast
  full_t1post <- tryCatch(
    gam(value ~ s(age, k = 4) + sex + s(t1post_dwi_contrast, k = 4) + batch_device_software, data = this_dat),
    error = function(e) NULL
  )
  qc_effect_t1post <- NA_real_
  if (!is.null(full_t1post)) {
    gam_summary_t1post <- summary(full_t1post)
    r2_adj_t1post <- adj_r2(gam_summary_t1post, n_obs)
    qc_effect_t1post <- r2_adj_t1post - r2_adj_red
  }

  # Full with mean_rating
  full_mean_rating <- tryCatch(
    gam(value ~ s(age, k = 4) + sex + s(mean_rating, k = 4) + batch_device_software, data = this_dat),
    error = function(e) NULL
  )
  qc_effect_mean_rating <- NA_real_
  if (!is.null(full_mean_rating)) {
    gam_summary_mr <- summary(full_mean_rating)
    r2_adj_mr <- adj_r2(gam_summary_mr, n_obs)
    qc_effect_mean_rating <- r2_adj_mr - r2_adj_red
  }

    results[[length(results) + 1]] <- tibble(
      bundle = bundle,
      bundle_category = bundle_category,
      metric = metric,
      qc_metric = "t1post_dwi_contrast",
      vendor = vendor,
      n_obs = n_obs,
      r2_adj_reduced = r2_adj_red,
      qc_effect_size = qc_effect_t1post,
      subset = "manual_rated"
    )
    results[[length(results) + 1]] <- tibble(
      bundle = bundle,
      bundle_category = bundle_category,
      metric = metric,
      qc_metric = "mean_rating",
      vendor = vendor,
      n_obs = n_obs,
      r2_adj_reduced = r2_adj_red,
      qc_effect_size = qc_effect_mean_rating,
      subset = "manual_rated"
    )
  }
}

res <- bind_rows(results)

if (nrow(res) > 0) {
  saveRDS(res, out_file)
  log_info("Saved manual-rated quality effects for metric:", metric, "| file:", out_file)
} else {
  log_info("No valid results for metric:", metric)
}
