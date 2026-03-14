#!/usr/bin/env Rscript

# ============================================================
# CALCULATE BATCH EFFECTS BY VENDOR (RAW AND HARMONIZED)
# ============================================================
# Purpose:
#   Run one (metric x qc_metric) task. For each vendor (GE, Philips, Siemens),
#   fit batch effects on raw and harmonized data; write one RDS per vendor
#   with source = "raw" or "harmonized".
#   Full: value ~ s(age,k=4) + sex + s(qc_var,k=4) + batch_device_software.
#   Reduced: value ~ s(age,k=4) + sex + s(qc_var,k=4).
#   Supports no_quality (no QC term in model). Same task space as main batch_effects.
# ============================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(stringr)
  library(tibble)
  library(fs)
  library(gamm4)
})

log_info <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n")
}

safe_numeric <- function(x) {
  x_num <- suppressWarnings(as.numeric(x))
  x_num[is.nan(x_num)] <- NA_real_
  x_num
}

extract_model_diagnostics <- function(mod) {
  lme4_msgs <- tryCatch(mod$mer@optinfo$conv$lme4$messages, error = function(e) NULL)
  opt_msg <- tryCatch(mod$mer@optinfo$conv$opt$message, error = function(e) NULL)
  msg_vec <- c(lme4_msgs, opt_msg)
  msg_vec <- msg_vec[!is.na(msg_vec) & nzchar(msg_vec)]

  list(
    aic = tryCatch(as.numeric(AIC(mod$mer)), error = function(e) NA_real_),
    lme4_converged = length(lme4_msgs) == 0,
    gam_converged = isTRUE(mod$gam$converged),
    singular_fit = tryCatch(lme4::isSingular(mod$mer, tol = 1e-4), error = function(e) NA),
    conv_message = if (length(msg_vec) == 0) NA_character_ else paste(msg_vec, collapse = " | ")
  )
}

fit_gamm_with_fallback <- function(formula, data) {
  mod <- tryCatch(
    gamm4(formula = formula, random = ~(1 + age | subject_id), data = data),
    error = function(e) NULL
  )
  if (!is.null(mod)) return(mod)
  tryCatch(
    gamm4(formula = formula, random = ~(1 | subject_id), data = data),
    error = function(e) NULL
  )
}

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  metric <- NULL
  qc_metric <- NULL

  i <- 1
  while (i <= length(args)) {
    if (args[i] == "--metric" && i + 1 <= length(args)) {
      metric <- args[i + 1]
      i <- i + 2
    } else if (args[i] == "--qc_metric" && i + 1 <= length(args)) {
      qc_metric <- args[i + 1]
      i <- i + 2
    } else {
      i <- i + 1
    }
  }

  if (!nzchar(metric) || !nzchar(qc_metric)) {
    stop("Usage: calculate_batch_effects_vendorwise.R --metric <metric> --qc_metric <qc_metric>")
  }

  list(metric = metric, qc_metric = qc_metric)
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
qc_metrics <- trimws(as.character(config$image_quality_metrics))

task <- parse_args()
metric <- trimws(task$metric)
qc_metric <- trimws(task$qc_metric)
is_no_quality <- identical(qc_metric, "no_quality")
vendors <- c("GE", "Philips", "Siemens")

if (!metric %in% metrics) {
  stop("Metric not in config microstructural_metrics: ", metric)
}
if (!is_no_quality && !qc_metric %in% qc_metrics) {
  stop("QC metric not in config image_quality_metrics: ", qc_metric)
}

log_info("Running vendorwise batch effects for metric:", metric, "| qc_metric:", qc_metric)

raw_file <- fs::path(PROJECT_ROOT, "data", "raw_data", "merged_data_meisler_analyses.parquet")
harmonized_file <- fs::path(PROJECT_ROOT, "data", "harmonized_data", "merged_data_meisler_analyses_harmonized.parquet")
out_dir <- fs::path(PROJECT_ROOT, "data", "batch_effects", "batch_effects_outputs_vendorwise")
dir_create(out_dir, recurse = TRUE)

if (!file_exists(raw_file)) {
  stop("Raw parquet not found: ", raw_file)
}
if (!file_exists(harmonized_file)) {
  stop("Harmonized parquet not found: ", harmonized_file)
}

qc_safe <- gsub("[^A-Za-z0-9_]+", "_", qc_metric)
out_files <- fs::path(out_dir, paste0(metric, "__", qc_safe, "__", vendors, "_batch_effects_by_vendor.rds"))
if (all(file_exists(out_files))) {
  log_info("All vendor outputs already exist, skipping.")
  quit(status = 0)
}

required_covars <- c("subject_id", "scanner_manufacturer", "batch_device_software", "sex", "age")
if (!is_no_quality) {
  required_covars <- c(required_covars, qc_metric)
}

df_harm <- read_parquet(harmonized_file)
df_raw <- read_parquet(raw_file)

if (!"batch_device_software" %in% names(df_harm)) {
  stop("Column batch_device_software not found in harmonized parquet.")
}
if (!is_no_quality && !qc_metric %in% names(df_harm)) {
  log_info("QC metric not in parquet:", qc_metric, "- skipping.")
  quit(status = 0)
}

harm_bundle_cols <- grep(
  paste0("^bundle_.+_", metric, "_median$"),
  names(df_harm),
  value = TRUE
)
if (length(harm_bundle_cols) == 0) {
  log_info("No bundle columns for metric:", metric, "- exiting.")
  quit(status = 0)
}

raw_bundle_cols <- intersect(harm_bundle_cols, names(df_raw))

run_for_vendor <- function(df_all, vendor, metric, qc_covariate, bundle_cols, no_quality, source_label) {
  df_vendor <- df_all %>% filter(as.character(scanner_manufacturer) == vendor)
  if (nrow(df_vendor) < 50) {
    log_info("Too few rows for vendor ", vendor, " - skipping.")
    return(tibble())
  }
  log_info("Processing vendor:", vendor, "| bundles:", length(bundle_cols))
  results <- list()

  for (bundle_col in sort(bundle_cols)) {
    log_info("  bundle:", bundle_col)
    full_bundle <- str_replace(bundle_col, paste0("^bundle_(.+)_", metric, "_median$"), "\\1")
    bundle_category <- str_extract(full_bundle, "^[^_]+")
    bundle <- str_replace(full_bundle, "^[^_]+_", "")

    if (no_quality) {
      this_dat <- df_vendor %>%
        select(all_of(required_covars), value = !!sym(bundle_col)) %>%
        mutate(
          age = safe_numeric(age),
          value = safe_numeric(value),
          subject_id = factor(subject_id),
          sex = factor(sex),
          scanner_manufacturer = factor(scanner_manufacturer),
          batch_device_software = factor(batch_device_software)
        ) %>%
        filter(
          !is.na(value),
          !is.na(batch_device_software),
          !is.na(age),
          !is.na(sex)
        )
    } else {
      this_dat <- df_vendor %>%
        select(all_of(required_covars), value = !!sym(bundle_col), qc_var = !!sym(qc_covariate)) %>%
        mutate(
          age = safe_numeric(age),
          value = safe_numeric(value),
          qc_var = safe_numeric(qc_var),
          subject_id = factor(subject_id),
          sex = factor(sex),
          scanner_manufacturer = factor(scanner_manufacturer),
          batch_device_software = factor(batch_device_software)
        ) %>%
        filter(
          !is.na(value),
          !is.na(batch_device_software),
          !is.na(age),
          !is.na(sex),
          !is.na(qc_var)
        )
    }

    if (nrow(this_dat) < 50) next

    if (no_quality) {
      full_mod <- fit_gamm_with_fallback(
        value ~ s(age, k = 4) + sex + batch_device_software,
        this_dat
      )
      red_mod <- fit_gamm_with_fallback(
        value ~ s(age, k = 4) + sex,
        this_dat
      )
    } else {
      full_mod <- fit_gamm_with_fallback(
        value ~ s(age, k = 4) + sex + s(qc_var, k = 4) + batch_device_software,
        this_dat
      )
      red_mod <- fit_gamm_with_fallback(
        value ~ s(age, k = 4) + sex + s(qc_var, k = 4),
        this_dat
      )
    }

    if (is.null(full_mod) || is.null(red_mod)) next

    gam_summary_full <- summary(full_mod$gam)
    gam_summary_red <- summary(red_mod$gam)
    r2_full <- gam_summary_full$r.sq
    r2_red <- gam_summary_red$r.sq
    delta_r2 <- r2_full - r2_red

    full_diag <- extract_model_diagnostics(full_mod)
    red_diag <- extract_model_diagnostics(red_mod)
    delta_aic <- red_diag$aic - full_diag$aic

    batch_p <- tryCatch({
      coefs <- summary(full_mod$gam)$p.table
      batch_rows <- grep("^batch_device_software", rownames(coefs))
      if (length(batch_rows) > 0) {
        max(coefs[batch_rows, "Pr(>|t|)"], na.rm = TRUE)
      } else {
        NA_real_
      }
    }, error = function(e) NA_real_)

    results[[length(results) + 1]] <- tibble(
      source = source_label,
      scanner_manufacturer = vendor,
      bundle = bundle,
      bundle_category = bundle_category,
      metric = metric,
      qc_covariate = qc_covariate,
      n_obs = nrow(this_dat),
      p_batch = batch_p,
      r2_full = r2_full,
      r2_reduced = r2_red,
      effect_size = delta_r2,
      aic_full = full_diag$aic,
      aic_reduced = red_diag$aic,
      delta_aic = delta_aic,
      full_lme4_converged = full_diag$lme4_converged,
      reduced_lme4_converged = red_diag$lme4_converged,
      full_gam_converged = full_diag$gam_converged,
      reduced_gam_converged = red_diag$gam_converged,
      full_singular_fit = full_diag$singular_fit,
      reduced_singular_fit = red_diag$singular_fit,
      full_conv_message = full_diag$conv_message,
      reduced_conv_message = red_diag$conv_message
    )
  }
  bind_rows(results)
}

for (v in vendors) {
  out_file <- fs::path(out_dir, paste0(metric, "__", qc_safe, "__", v, "_batch_effects_by_vendor.rds"))
  if (file_exists(out_file)) {
    log_info("Output already exists, skipping vendor:", v)
    next
  }
  res_harm <- run_for_vendor(df_harm, v, metric, qc_metric, harm_bundle_cols, is_no_quality, "harmonized")
  res_raw <- if (length(raw_bundle_cols) > 0 && "batch_device_software" %in% names(df_raw)) {
    run_for_vendor(df_raw, v, metric, qc_metric, raw_bundle_cols, is_no_quality, "raw")
  } else {
    tibble()
  }
  res <- bind_rows(res_harm, res_raw)
  if (nrow(res) > 0) {
    saveRDS(res, out_file)
    log_info("Saved batch effects for metric:", metric, "| qc_metric:", qc_metric, "| vendor:", v, "| file:", out_file)
  } else {
    log_info("No valid results for metric:", metric, "| qc_metric:", qc_metric, "| vendor:", v)
  }
}
