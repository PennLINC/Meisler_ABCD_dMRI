#!/usr/bin/env Rscript

# ============================================================
# CALCULATE AGE + QUALITY EFFECTS BY VENDOR (TASK MODE)
# ============================================================
# Purpose:
#   Run one metric x one QC-metric task (from CLI args), fitting all bundles
#   for that metric separately within each scanner manufacturer for raw and
#   harmonized data.
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
    stop("Usage: calculate_age_quality_effects_vendorwise.R --metric <metric> --qc_metric <qc_metric>")
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

log_info("Running vendorwise age-quality task for metric:", metric, "| qc_metric:", qc_metric)

raw_file <- fs::path(PROJECT_ROOT, "data", "raw_data", "merged_data_meisler_analyses.parquet")
harmonized_file <- fs::path(PROJECT_ROOT, "data", "harmonized_data", "merged_data_meisler_analyses_harmonized.parquet")
out_dir <- fs::path(PROJECT_ROOT, "data", "age_quality_effects", "age_quality_effects_outputs_vendorwise")
dir_create(out_dir, recurse = TRUE)

if (!file_exists(raw_file)) {
  stop("Raw input parquet not found: ", raw_file)
}
if (!file_exists(harmonized_file)) {
  stop("Assembled harmonized parquet not found: ", harmonized_file)
}

qc_safe <- gsub("[^A-Za-z0-9_]+", "_", qc_metric)
out_file <- fs::path(out_dir, paste0(metric, "__", qc_safe, "_age_quality_effects_by_vendor.rds"))
if (file_exists(out_file)) {
  log_info("Output already exists, skipping:", out_file)
  quit(status = 0)
}

required_covars <- c("subject_id", "scanner_manufacturer", "sex", "age")

run_pair_for_source <- function(df, source_label, metric, qc_metric, bundle_cols = NULL) {
  if (!is_no_quality && !qc_metric %in% names(df)) {
    log_info("QC metric not found in", source_label, "parquet:", qc_metric, "- skipping source.")
    return(tibble())
  }

  if (is.null(bundle_cols)) {
    bundle_cols <- grep(
      paste0("^bundle_.+_", metric, "_median$"),
      names(df),
      value = TRUE
    )
  }
  if (length(bundle_cols) == 0) {
    log_info("No", source_label, "bundle columns found for", metric, "- skipping source.")
    return(tibble())
  }

  vendors <- sort(unique(as.character(df$scanner_manufacturer)))
  vendors <- vendors[!is.na(vendors) & nzchar(vendors)]
  if (length(vendors) == 0) {
    log_info("No valid vendors found in", source_label, "data.")
    return(tibble())
  }

  results <- list()

  for (vendor in vendors) {
    log_info("Modeling vendor:", vendor, "[", source_label, "]")

    df_vendor <- df %>%
      filter(as.character(scanner_manufacturer) == vendor)

    if (nrow(df_vendor) < 50) {
      next
    }

    for (bundle_col in sort(bundle_cols)) {
      log_info("Processing bundle column:", bundle_col, "| vendor:", vendor, "| source:", source_label)

      full_bundle <- str_replace(bundle_col, paste0("^bundle_(.+)_", metric, "_median$"), "\\1")
      bundle_category <- str_extract(full_bundle, "^[^_]+")
      bundle <- str_replace(full_bundle, "^[^_]+_", "")

      if (is_no_quality) {
        this_dat <- df_vendor %>%
          select(all_of(required_covars), value = !!sym(bundle_col)) %>%
          mutate(
            age = safe_numeric(age),
            value = safe_numeric(value),
            subject_id = factor(subject_id),
            sex = factor(sex),
            scanner_manufacturer = factor(scanner_manufacturer)
          ) %>%
          filter(!is.na(value), !is.na(age), !is.na(sex))
      } else {
        this_dat <- df_vendor %>%
          select(all_of(required_covars), value = !!sym(bundle_col), qc_var = !!sym(qc_metric)) %>%
          mutate(
            age = safe_numeric(age),
            value = safe_numeric(value),
            qc_var = safe_numeric(qc_var),
            subject_id = factor(subject_id),
            sex = factor(sex),
            scanner_manufacturer = factor(scanner_manufacturer)
          ) %>%
          filter(!is.na(value), !is.na(age), !is.na(sex), !is.na(qc_var))
      }

      if (nrow(this_dat) < 50) {
        next
      }

      age_min <- suppressWarnings(quantile(this_dat$age, 0.025, na.rm = TRUE))
      age_max <- suppressWarnings(quantile(this_dat$age, 0.975, na.rm = TRUE))

      if (is_no_quality) {
        full_mod <- fit_gamm_with_fallback(
          value ~ s(age, k = 4) + sex,
          this_dat
        )
        red_age_mod <- fit_gamm_with_fallback(
          value ~ sex,
          this_dat
        )
        red_qc_mod <- NULL
      } else {
        full_mod <- fit_gamm_with_fallback(
          value ~ s(age, k = 4) + sex + s(qc_var, k = 4),
          this_dat
        )
        red_age_mod <- fit_gamm_with_fallback(
          value ~ sex + s(qc_var, k = 4),
          this_dat
        )
        red_qc_mod <- fit_gamm_with_fallback(
          value ~ s(age, k = 4) + sex,
          this_dat
        )
      }

      if (is.null(full_mod) || is.null(red_age_mod) || (!is_no_quality && is.null(red_qc_mod))) {
        next
      }

      gam_summary_full <- summary(full_mod$gam)
      gam_summary_red_age <- summary(red_age_mod$gam)
      gam_summary_red_qc <- if (!is_no_quality) summary(red_qc_mod$gam) else NULL

      r2_full <- gam_summary_full$r.sq
      r2_red_age <- gam_summary_red_age$r.sq
      r2_red_qc <- if (!is_no_quality) gam_summary_red_qc$r.sq else NA_real_

      age_row <- grep("^s\\(age\\)", rownames(gam_summary_full$s.table))
      qc_row <- if (!is_no_quality) grep("^s\\(qc_var\\)", rownames(gam_summary_full$s.table)) else integer(0)

      p_age <- tryCatch(
        if (length(age_row) > 0) gam_summary_full$s.table[age_row[1], "p-value"] else NA_real_,
        error = function(e) NA_real_
      )
      edf_age <- tryCatch(
        if (length(age_row) > 0) gam_summary_full$s.table[age_row[1], "edf"] else NA_real_,
        error = function(e) NA_real_
      )
      p_qc <- if (!is_no_quality) {
        tryCatch(
          if (length(qc_row) > 0) gam_summary_full$s.table[qc_row[1], "p-value"] else NA_real_,
          error = function(e) NA_real_
        )
      } else {
        NA_real_
      }
      edf_qc <- if (!is_no_quality) {
        tryCatch(
          if (length(qc_row) > 0) gam_summary_full$s.table[qc_row[1], "edf"] else NA_real_,
          error = function(e) NA_real_
        )
      } else {
        NA_real_
      }

      if (is_no_quality) {
        newdat_age <- data.frame(
          age = c(age_min, age_max),
          sex = levels(this_dat$sex)[1]
        )
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

      # Age effect from model WITHOUT QC (for scatter: age slope no QC vs with QC)
      newdat_age_no_qc <- data.frame(
        age = c(age_min, age_max),
        sex = levels(this_dat$sex)[1]
      )
      pred_age_no_qc <- if (!is_no_quality) {
        tryCatch(
          predict(red_qc_mod$gam, newdata = newdat_age_no_qc),
          error = function(e) c(NA_real_, NA_real_)
        )
      } else {
        pred_age
      }
      age_percent_change_no_qc <- if (is.finite(pred_age_no_qc[1]) && pred_age_no_qc[1] != 0) {
        100 * (pred_age_no_qc[2] - pred_age_no_qc[1]) / pred_age_no_qc[1]
      } else {
        NA_real_
      }

      qc_percent_change <- if (!is_no_quality) {
        qc_lo <- quantile(this_dat$qc_var, 0.10, na.rm = TRUE)
        qc_hi <- quantile(this_dat$qc_var, 0.90, na.rm = TRUE)
        qc_mid <- quantile(this_dat$qc_var, 0.50, na.rm = TRUE)
        newdat_qc <- data.frame(
          age = median(this_dat$age, na.rm = TRUE),
          sex = levels(this_dat$sex)[1],
          qc_var = c(qc_lo, qc_hi, qc_mid)
        )
        pred_qc <- tryCatch(
          predict(full_mod$gam, newdata = newdat_qc),
          error = function(e) c(NA_real_, NA_real_, NA_real_)
        )
        if (length(pred_qc) >= 3 && is.finite(pred_qc[3]) && pred_qc[3] != 0) {
          100 * (pred_qc[2] - pred_qc[1]) / pred_qc[3]
        } else {
          NA_real_
        }
      } else {
        NA_real_
      }

      full_diag <- extract_model_diagnostics(full_mod)
      red_age_diag <- extract_model_diagnostics(red_age_mod)
      red_qc_diag <- if (!is_no_quality) {
        extract_model_diagnostics(red_qc_mod)
      } else {
        list(
          aic = NA_real_,
          lme4_converged = NA,
          gam_converged = NA,
          singular_fit = NA,
          conv_message = NA_character_
        )
      }

      results[[length(results) + 1]] <- tibble(
        source = source_label,
        scanner_manufacturer = vendor,
        bundle = bundle,
        bundle_category = bundle_category,
        metric = metric,
        qc_metric = qc_metric,
        n_obs = nrow(this_dat),
        p_age_smooth = p_age,
        edf_age = edf_age,
        p_qc_smooth = p_qc,
        edf_qc = edf_qc,
        r2_full = r2_full,
        r2_reduced_age = r2_red_age,
        r2_reduced_qc = r2_red_qc,
        age_effect_size = r2_full - r2_red_age,
        qc_effect_size = if (!is_no_quality) r2_full - r2_red_qc else NA_real_,
        percent_change_age = age_percent_change,
        percent_change_age_no_qc = age_percent_change_no_qc,
        percent_change_qc = qc_percent_change,
        aic_full = full_diag$aic,
        aic_reduced_age = red_age_diag$aic,
        aic_reduced_qc = red_qc_diag$aic,
        delta_aic_age = red_age_diag$aic - full_diag$aic,
        delta_aic_qc = if (!is_no_quality) red_qc_diag$aic - full_diag$aic else NA_real_,
        full_lme4_converged = full_diag$lme4_converged,
        reduced_age_lme4_converged = red_age_diag$lme4_converged,
        reduced_qc_lme4_converged = red_qc_diag$lme4_converged,
        full_gam_converged = full_diag$gam_converged,
        reduced_age_gam_converged = red_age_diag$gam_converged,
        reduced_qc_gam_converged = red_qc_diag$gam_converged,
        full_singular_fit = full_diag$singular_fit,
        reduced_age_singular_fit = red_age_diag$singular_fit,
        reduced_qc_singular_fit = red_qc_diag$singular_fit,
        full_conv_message = full_diag$conv_message,
        reduced_age_conv_message = red_age_diag$conv_message,
        reduced_qc_conv_message = red_qc_diag$conv_message
      )
    }
  }

  bind_rows(results)
}

df_harm <- read_parquet(harmonized_file)
harm_bundle_cols <- grep(
  paste0("^bundle_.+_", metric, "_median$"),
  names(df_harm),
  value = TRUE
)
res_harm <- run_pair_for_source(df_harm, "harmonized", metric, qc_metric, bundle_cols = harm_bundle_cols)

df_raw <- read_parquet(raw_file)
raw_bundle_cols <- intersect(harm_bundle_cols, names(df_raw))
res_raw <- run_pair_for_source(df_raw, "raw", metric, qc_metric, bundle_cols = raw_bundle_cols)

res_all <- bind_rows(res_raw, res_harm)

if (nrow(res_all) > 0) {
  saveRDS(res_all, out_file)
  log_info("Saved vendorwise age-quality effects for metric:", metric, "| qc_metric:", qc_metric)
  log_info("Output file:", out_file)
} else {
  log_info("No valid vendorwise age-quality results for metric:", metric, "| qc_metric:", qc_metric)
}
