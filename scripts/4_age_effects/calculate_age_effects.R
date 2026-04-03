#!/usr/bin/env Rscript

# ============================================================
# CALCULATE AGE EFFECTS (POOLED, HARMONIZED ONLY)
# ============================================================
# Purpose:
#   Run one (metric x qc_metric) task. Full GAM: value ~ s(age, k=4) + sex + s(quality_covariate, k=4)
#   (or value ~ s(age, k=4) + sex if qc_metric == no_quality). No batch.
#   Reduced model drops s(age) to compute age effect. No quality effect / reduced model.
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
    stop("Usage: calculate_age_effects.R --metric <metric> --qc_metric <qc_metric|no_quality>")
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
qc_metrics_config <- trimws(as.character(config$image_quality_metrics))

task <- parse_args()
metric <- trimws(task$metric)
qc_metric <- trimws(task$qc_metric)
qc_metric_norm <- trimws(tolower(as.character(qc_metric)))
is_no_quality <- (qc_metric_norm == "no_quality")

if (!metric %in% metrics) {
  stop("Metric not in config microstructural_metrics: ", metric)
}
if (!is_no_quality && !qc_metric %in% qc_metrics_config) {
  stop("QC metric not in config image_quality_metrics: ", qc_metric)
}

log_info("Running pooled age effects for metric:", metric, "| qc_metric:", qc_metric)

harmonized_file <- fs::path(PROJECT_ROOT, "data", "harmonized_data", "merged_data_meisler_analyses_harmonized.parquet")
out_dir <- fs::path(PROJECT_ROOT, "data", "age_effects", "age_effects_outputs")
dir_create(out_dir, recurse = TRUE)

if (!file_exists(harmonized_file)) {
  stop("Harmonized parquet not found: ", harmonized_file)
}

qc_safe <- gsub("[^A-Za-z0-9_]+", "_", qc_metric)
out_file <- fs::path(out_dir, paste0(metric, "__", qc_safe, "_age_effects.rds"))
if (file_exists(out_file)) {
  log_info("Output already exists, skipping:", out_file)
  quit(status = 0)
}

required_covars <- c("subject_id", "sex", "age")
df <- read_parquet(harmonized_file)

if (!is_no_quality && !qc_metric %in% names(df)) {
  log_info("QC metric not in parquet:", qc_metric, "- skipping.")
  quit(status = 0)
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

results <- list()

for (bundle_col in sort(bundle_cols)) {
  log_info("Processing bundle column:", bundle_col)

  full_bundle <- str_replace(bundle_col, paste0("^bundle_(.+)_", metric, "_median$"), "\\1")
  bundle_category <- str_extract(full_bundle, "^[^_]+")
  bundle <- str_replace(full_bundle, "^[^_]+_", "")

  if (is_no_quality) {
    this_dat <- df %>%
      select(all_of(required_covars), value = !!sym(bundle_col)) %>%
      mutate(
        age = safe_numeric(age),
        value = safe_numeric(value),
        subject_id = factor(subject_id),
        sex = factor(sex)
      ) %>%
      filter(!is.na(value), !is.na(age), !is.na(sex))
  } else {
    this_dat <- df %>%
      select(all_of(required_covars), value = !!sym(bundle_col), qc_var = !!sym(qc_metric)) %>%
      mutate(
        age = safe_numeric(age),
        value = safe_numeric(value),
        qc_var = safe_numeric(qc_var),
        subject_id = factor(subject_id),
        sex = factor(sex)
      ) %>%
      filter(!is.na(value), !is.na(age), !is.na(sex), !is.na(qc_var))
  }

  if (nrow(this_dat) < 50) {
    next
  }

  age_min <- suppressWarnings(quantile(this_dat$age, 0.025, na.rm = TRUE))
  age_max <- suppressWarnings(quantile(this_dat$age, 0.975, na.rm = TRUE))

  if (is_no_quality) {
    full_mod <- fit_gamm_with_fallback(value ~ s(age, k = 4) + sex, this_dat)
    red_age_mod <- fit_gamm_with_fallback(value ~ sex, this_dat)
  } else {
    full_mod <- fit_gamm_with_fallback(value ~ s(age, k = 4) + sex + s(qc_var, k = 4), this_dat)
    red_age_mod <- fit_gamm_with_fallback(value ~ sex + s(qc_var, k = 4), this_dat)
  }

  if (is.null(full_mod) || is.null(red_age_mod)) {
    next
  }

  gam_summary_full <- summary(full_mod$gam)
  gam_summary_red_age <- summary(red_age_mod$gam)

  r2_full <- gam_summary_full$r.sq
  r2_red_age <- gam_summary_red_age$r.sq

  age_row <- grep("^s\\(age\\)", rownames(gam_summary_full$s.table))
  p_age <- tryCatch(
    if (length(age_row) > 0) gam_summary_full$s.table[age_row[1], "p-value"] else NA_real_,
    error = function(e) NA_real_
  )
  edf_age <- tryCatch(
    if (length(age_row) > 0) gam_summary_full$s.table[age_row[1], "edf"] else NA_real_,
    error = function(e) NA_real_
  )

  if (is_no_quality) {
    newdat_age <- data.frame(age = c(age_min, age_max), sex = levels(this_dat$sex)[1])
  } else {
    newdat_age <- data.frame(
      age = c(age_min, age_max),
      sex = levels(this_dat$sex)[1],
      qc_var = mean(this_dat$qc_var, na.rm = TRUE)
    )
  }
  pred_age <- tryCatch(
    predict(full_mod$gam, newdata = newdat_age),
    error = function(e) c(NA_real_, NA_real_)
  )
  delta_age <- pred_age[2] - pred_age[1]
  age_percent_change <- if (is.finite(pred_age[1]) && pred_age[1] != 0) {
    100 * (delta_age / pred_age[1])
  } else {
    NA_real_
  }

  full_diag <- extract_model_diagnostics(full_mod)
  red_age_diag <- extract_model_diagnostics(red_age_mod)

  results[[length(results) + 1]] <- tibble(
    bundle = bundle,
    bundle_category = bundle_category,
    metric = metric,
    qc_metric = qc_metric,
    n_obs = nrow(this_dat),
    p_age_smooth = p_age,
    edf_age = edf_age,
    r2_full = r2_full,
    r2_reduced_age = r2_red_age,
    age_effect_size = r2_full - r2_red_age,
    percent_change_age = age_percent_change,
    aic_full = full_diag$aic,
    aic_reduced_age = red_age_diag$aic,
    delta_aic_age = red_age_diag$aic - full_diag$aic,
    full_lme4_converged = full_diag$lme4_converged,
    reduced_age_lme4_converged = red_age_diag$lme4_converged,
    full_gam_converged = full_diag$gam_converged,
    reduced_age_gam_converged = red_age_diag$gam_converged,
    full_singular_fit = full_diag$singular_fit,
    reduced_age_singular_fit = red_age_diag$singular_fit,
    full_conv_message = full_diag$conv_message,
    reduced_age_conv_message = red_age_diag$conv_message
  )
}

res <- bind_rows(results)

if (nrow(res) > 0) {
  saveRDS(res, out_file)
  log_info("Saved pooled age effects for metric:", metric, "| qc_metric:", qc_metric, "| file:", out_file)
} else {
  log_info("No valid pooled age effects for metric:", metric, "| qc_metric:", qc_metric)
}
