#!/usr/bin/env Rscript

# ============================================================
# CALCULATE QUALITY EFFECTS (POOLED, HARMONIZED ONLY)
# ============================================================
# Purpose:
#   Run one (metric x IQM) task. Full: value ~ s(age,k=4) + sex + s(qc_var,k=4) + batch.
#   Reduced for quality effect: remove s(qc_var) -> value ~ s(age,k=4) + sex + batch.
#   Loops over microstructural_metrics x image_quality_metrics only (no no_quality).
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
    stop("Usage: calculate_quality_effects.R --metric <metric> --qc_metric <qc_metric>")
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

if (!metric %in% metrics) {
  stop("Metric not in config microstructural_metrics: ", metric)
}
if (!qc_metric %in% qc_metrics) {
  stop("QC metric not in config image_quality_metrics (no no_quality): ", qc_metric)
}

log_info("Running pooled quality effects for metric:", metric, "| qc_metric:", qc_metric)

harmonized_file <- fs::path(PROJECT_ROOT, "data", "harmonized_data", "merged_data_meisler_analyses_harmonized.parquet")
out_dir <- fs::path(PROJECT_ROOT, "data", "quality_effects", "quality_effects_outputs")
dir_create(out_dir, recurse = TRUE)

if (!file_exists(harmonized_file)) {
  stop("Assembled harmonized parquet not found: ", harmonized_file)
}

qc_safe <- gsub("[^A-Za-z0-9_]+", "_", qc_metric)
out_file <- fs::path(out_dir, paste0(metric, "__", qc_safe, "_quality_effects.rds"))
if (file_exists(out_file)) {
  log_info("Output already exists, skipping:", out_file)
  quit(status = 0)
}

required_covars <- c("subject_id", "batch_device_software", "sex", "age")
df <- read_parquet(harmonized_file)

if (!qc_metric %in% names(df)) {
  log_info("QC metric not found in harmonized parquet:", qc_metric, "- skipping task.")
  quit(status = 0)
}
if (!"batch_device_software" %in% names(df)) {
  stop("Column batch_device_software not found in harmonized parquet. It is added in data prep (filter_data_meisler_analyses.R) and retained in assemble_harmonized_data.R.")
}

bundle_cols <- grep(
  paste0("^bundle_.+_", metric, "_median$"),
  names(df),
  value = TRUE
)
if (length(bundle_cols) == 0) {
  log_info("No harmonized bundle columns found for", metric, "- exiting.")
  quit(status = 0)
}

log_info("Processing", length(bundle_cols), "bundles for metric:", metric)
results <- list()

for (bundle_col in sort(bundle_cols)) {
  log_info("  bundle:", bundle_col)

  full_bundle <- str_replace(bundle_col, paste0("^bundle_(.+)_", metric, "_median$"), "\\1")
  bundle_category <- str_extract(full_bundle, "^[^_]+")
  bundle <- str_replace(full_bundle, "^[^_]+_", "")

  this_dat <- df %>%
    select(all_of(required_covars), value = !!sym(bundle_col), qc_var = !!sym(qc_metric)) %>%
    mutate(
      age = safe_numeric(age),
      value = safe_numeric(value),
      qc_var = safe_numeric(qc_var),
      subject_id = factor(subject_id),
      batch_device_software = factor(batch_device_software),
      sex = factor(sex)
    ) %>%
    filter(!is.na(value), !is.na(age), !is.na(batch_device_software), !is.na(sex), !is.na(qc_var))

  if (nrow(this_dat) < 50) {
    next
  }

  full_mod <- tryCatch(
    gamm4(
      formula = value ~ s(age, k = 4) + sex + s(qc_var, k = 4) + batch_device_software,
      random = ~(1 + age | subject_id),
      data = this_dat
    ),
    error = function(e) NULL
  )
  red_qc_mod <- tryCatch(
    gamm4(
      formula = value ~ s(age, k = 4) + sex + batch_device_software,
      random = ~(1 + age | subject_id),
      data = this_dat
    ),
    error = function(e) NULL
  )

  if (is.null(full_mod) || is.null(red_qc_mod)) {
    next
  }

  gam_summary_full <- summary(full_mod$gam)
  gam_summary_red_qc <- summary(red_qc_mod$gam)

  r2_full <- gam_summary_full$r.sq
  r2_red_qc <- gam_summary_red_qc$r.sq

  qc_row <- grep("^s\\(qc_var\\)", rownames(gam_summary_full$s.table))

  p_qc <- tryCatch(
    if (length(qc_row) > 0) gam_summary_full$s.table[qc_row[1], "p-value"] else NA_real_,
    error = function(e) NA_real_
  )
  edf_qc <- tryCatch(
    if (length(qc_row) > 0) gam_summary_full$s.table[qc_row[1], "edf"] else NA_real_,
    error = function(e) NA_real_
  )

  qc_lo <- quantile(this_dat$qc_var, 0.10, na.rm = TRUE)
  qc_hi <- quantile(this_dat$qc_var, 0.90, na.rm = TRUE)
  qc_mid <- quantile(this_dat$qc_var, 0.50, na.rm = TRUE)
  newdat_qc <- data.frame(
    age = median(this_dat$age, na.rm = TRUE),
    sex = levels(this_dat$sex)[1],
    qc_var = c(qc_lo, qc_hi, qc_mid),
    batch_device_software = levels(this_dat$batch_device_software)[1]
  )
  pred_qc <- tryCatch(
    predict(full_mod$gam, newdata = newdat_qc),
    error = function(e) c(NA_real_, NA_real_, NA_real_)
  )
  qc_percent_change <- if (length(pred_qc) >= 3 && is.finite(pred_qc[3]) && pred_qc[3] != 0) {
    100 * (pred_qc[2] - pred_qc[1]) / pred_qc[3]
  } else {
    NA_real_
  }

  full_diag <- extract_model_diagnostics(full_mod)
  red_qc_diag <- extract_model_diagnostics(red_qc_mod)

  results[[length(results) + 1]] <- tibble(
    bundle = bundle,
    bundle_category = bundle_category,
    metric = metric,
    qc_metric = qc_metric,
    n_obs = nrow(this_dat),
    p_qc_smooth = p_qc,
    edf_qc = edf_qc,
    r2_full = r2_full,
    r2_reduced_qc = r2_red_qc,
    qc_effect_size = r2_full - r2_red_qc,
    percent_change_qc = qc_percent_change,
    aic_full = full_diag$aic,
    aic_reduced_qc = red_qc_diag$aic,
    delta_aic_qc = red_qc_diag$aic - full_diag$aic,
    full_lme4_converged = full_diag$lme4_converged,
    reduced_qc_lme4_converged = red_qc_diag$lme4_converged,
    full_gam_converged = full_diag$gam_converged,
    reduced_qc_gam_converged = red_qc_diag$gam_converged,
    full_singular_fit = full_diag$singular_fit,
    reduced_qc_singular_fit = red_qc_diag$singular_fit,
    full_conv_message = full_diag$conv_message,
    reduced_qc_conv_message = red_qc_diag$conv_message
  )
}

res <- bind_rows(results)

if (nrow(res) > 0) {
  saveRDS(res, out_file)
  log_info("Saved quality effects for metric:", metric, "| qc_metric:", qc_metric, "| file:", out_file)
} else {
  log_info("No valid results for metric:", metric, "| qc_metric:", qc_metric)
}

