#!/usr/bin/env Rscript

# ============================================================
# CALCULATE BATCH EFFECTS (TASK MODE, NONLINEAR GAMM)
# ============================================================
# Purpose:
#   Run one metric x one QC-metric task (from CLI args), estimating batch
#   effects using delta R^2 between:
#     - full model:    value ~ s(age) + sex + s(qc_var) + batch_device_software
#     - reduced model: value ~ s(age) + sex + s(qc_var)
#   Runs on both raw and assembled harmonized data.
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
    stop("Usage: calculate_batch_effects.R --metric <metric> --qc_metric <qc_metric>")
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

if (!metric %in% metrics) {
  stop("Metric not in config microstructural_metrics: ", metric)
}
if (!is_no_quality && !qc_metric %in% qc_metrics) {
  stop("QC metric not in config image_quality_metrics: ", qc_metric)
}

log_info("Running batch-effects GAMM for metric:", metric, "| qc_metric:", qc_metric)

raw_file <- fs::path(PROJECT_ROOT, "data", "raw_data", "merged_data_meisler_analyses.parquet")
harmonized_file <- fs::path(PROJECT_ROOT, "data", "harmonized_data", "merged_data_meisler_analyses_harmonized.parquet")
out_dir <- fs::path(PROJECT_ROOT, "data", "batch_effects", "batch_effects_outputs")
dir_create(out_dir, recurse = TRUE)

if (!file_exists(raw_file)) {
  stop("Raw input parquet not found: ", raw_file)
}
if (!file_exists(harmonized_file)) {
  stop("Assembled harmonized parquet not found: ", harmonized_file)
}

qc_safe <- gsub("[^A-Za-z0-9_]+", "_", qc_metric)
out_file <- fs::path(out_dir, paste0(metric, "__", qc_safe, "_batch_effects.rds"))
if (file_exists(out_file)) {
  log_info("Output already exists, skipping:", out_file)
  quit(status = 0)
}

run_batch_effect <- function(df, bundle_cols, metric, qc_covariate, source_label) {
  results <- list()
  required_covars <- c("subject_id", "sex", "age", "batch_device_software")
  if (!is_no_quality) {
    required_covars <- c(required_covars, qc_covariate)
  }

  if (!is_no_quality && !qc_covariate %in% names(df)) {
    log_info("QC metric not found in", source_label, "parquet:", qc_covariate, "- skipping source.")
    return(tibble())
  }
  if (!"batch_device_software" %in% names(df)) {
    log_info("batch_device_software not found in", source_label, "parquet - skipping source.")
    return(tibble())
  }

  for (col in sort(bundle_cols)) {
    log_info("Modeling bundle:", col, "[", source_label, "]")

    full_bundle <- str_replace(col, paste0("^bundle_(.+)_", metric, "_median$"), "\\1")
    bundle_category <- str_extract(full_bundle, "^[^_]+")
    bundle <- str_replace(full_bundle, "^[^_]+_", "")

    if (is_no_quality) {
      dat <- df %>%
        select(all_of(required_covars), value = !!sym(col)) %>%
        mutate(
          age = safe_numeric(age),
          value = safe_numeric(value),
          subject_id = factor(subject_id),
          sex = factor(sex),
          batch_device_software = factor(batch_device_software)
        ) %>%
        filter(
          !is.na(value),
          !is.na(batch_device_software),
          !is.na(age),
          !is.na(sex)
        )
    } else {
      dat <- df %>%
        select(all_of(required_covars), value = !!sym(col)) %>%
        mutate(
          age = safe_numeric(age),
          qc_covar = safe_numeric(.data[[qc_covariate]]),
          value = safe_numeric(value),
          subject_id = factor(subject_id),
          sex = factor(sex),
          batch_device_software = factor(batch_device_software)
        ) %>%
        filter(
          !is.na(value),
          !is.na(batch_device_software),
          !is.na(age),
          !is.na(sex),
          !is.na(qc_covar)
        )
    }

    if (nrow(dat) < 50) {
      next
    }

    if (is_no_quality) {
      full_mod <- tryCatch(
        gamm4(
          formula = value ~ s(age, k = 4) + sex + batch_device_software,
          random = ~(1 + age | subject_id),
          data = dat
        ),
        error = function(e) {
          log_info("Full model failed for", col, ":", e$message)
          NULL
        }
      )

      red_mod <- tryCatch(
        gamm4(
          formula = value ~ s(age, k = 4) + sex,
          random = ~(1 + age | subject_id),
          data = dat
        ),
        error = function(e) NULL
      )
    } else {
      full_mod <- tryCatch(
        gamm4(
          formula = value ~ s(age, k = 4) + sex + s(qc_covar, k = 4) + batch_device_software,
          random = ~(1 + age | subject_id),
          data = dat
        ),
        error = function(e) {
          log_info("Full model failed for", col, ":", e$message)
          NULL
        }
      )

      red_mod <- tryCatch(
        gamm4(
          formula = value ~ s(age, k = 4) + sex + s(qc_covar, k = 4),
          random = ~(1 + age | subject_id),
          data = dat
        ),
        error = function(e) NULL
      )
    }

    if (is.null(full_mod) || is.null(red_mod)) {
      next
    }

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
      bundle = bundle,
      bundle_category = bundle_category,
      metric = metric,
      source = source_label,
      qc_covariate = qc_covariate,
      n_obs = nrow(dat),
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

df_harm <- read_parquet(harmonized_file)
harm_bundle_cols <- grep(
  paste0("^bundle_.+_", metric, "_median$"),
  names(df_harm),
  value = TRUE
)

if (length(harm_bundle_cols) > 0) {
  res_harm <- run_batch_effect(df_harm, harm_bundle_cols, metric, qc_metric, "harmonized")
} else {
  log_info("No harmonized bundle columns found for", metric, "- skipping harmonized analysis.")
  res_harm <- tibble()
}

df_raw <- read_parquet(raw_file)
raw_bundle_cols <- intersect(harm_bundle_cols, names(df_raw))

if (length(raw_bundle_cols) > 0) {
  res_raw <- run_batch_effect(df_raw, raw_bundle_cols, metric, qc_metric, "raw")
} else {
  log_info(
    "No raw bundle columns matching harmonized bundle set found for",
    metric,
    "- skipping raw analysis."
  )
  res_raw <- tibble()
}

res_all <- bind_rows(res_raw, res_harm)

if (nrow(res_all) > 0) {
  saveRDS(res_all, out_file)
  log_info("Saved batch effects for metric:", metric, "| qc_metric:", qc_metric)
  log_info("Output file:", out_file)
} else {
  log_info("No valid results for metric:", metric, "| qc_metric:", qc_metric)
}
