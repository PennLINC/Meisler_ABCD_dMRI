#!/usr/bin/env Rscript
# =============================================================================
# Add QC effect to the assembled age-effects RDS.
#
# The age_effects scripts fit:
#   Full:    value ~ s(age) + sex + s(qc)   [or s(age) + sex when no_quality]
#   Reduced (age effect):  without s(age)
#
# The QC effect is: R²(full) - R²(model without s(qc)) = R²(s(age)+sex+s(qc)) - R²(s(age)+sex).
# R²(s(age)+sex) for each (metric, bundle, vendor, source) is exactly the
# r2_full from the no_quality age_effects run. So we join no_quality r2_full
# and set qc_effect_size = r2_full - r2_full_no_quality. Does not apply for
# no_quality rows (set to NA).
# =============================================================================

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
if (!file.exists(config_path)) {
  stop("CONFIG_PATH does not exist: ", config_path)
}

config <- jsonlite::fromJSON(config_path)
project_root <- normalizePath(config$project_root, mustWork = FALSE)
base_dir <- fs::path(project_root, "data", "age_effects")
rds_path <- fs::path(base_dir, "age_effects_all_outputs.rds")

if (!file.exists(rds_path)) {
  stop("Assembled age-effects RDS not found. Run assemble_age_effects.R first: ", rds_path)
}

log_info("Reading ", rds_path)
df <- readRDS(rds_path)
if (!is.data.frame(df)) {
  stop("File is not a data.frame: ", rds_path)
}

required <- c("metric", "qc_metric", "bundle", "r2_full", "output_type", "source")
if (!all(required %in% names(df))) {
  stop("Missing required columns: ", paste(setdiff(required, names(df)), collapse = ", "))
}

# Key for joining: same (output_type, source, scanner_manufacturer, metric, bundle).
# no_quality rows have r2_full = R²(s(age)+sex). Use that as R² of the "reduced QC" model.
join_cols <- c("output_type", "source", "scanner_manufacturer", "metric", "bundle")
if (!"scanner_manufacturer" %in% names(df)) {
  stop("Column scanner_manufacturer not found; cannot join no_quality R².")
}

no_quality_r2 <- df %>%
  filter(qc_metric == "no_quality", !is.na(r2_full)) %>%
  select(all_of(join_cols), r2_full) %>%
  rename(r2_full_no_qc = r2_full) %>%
  distinct()

log_info("No-quality reference rows (R² of s(age)+sex): ", nrow(no_quality_r2))

# If we already added qc_effect_size / r2_full_no_qc, drop so we don't duplicate
if ("r2_full_no_qc" %in% names(df)) df <- select(df, -r2_full_no_qc)
if ("qc_effect_size" %in% names(df)) df <- select(df, -qc_effect_size)

df <- df %>%
  left_join(no_quality_r2, by = join_cols) %>%
  mutate(
    qc_effect_size = if_else(
      qc_metric == "no_quality" | is.na(r2_full_no_qc),
      NA_real_,
      r2_full - r2_full_no_qc
    )
  )

saveRDS(df, rds_path)
log_info("Saved ", rds_path, " with columns: qc_effect_size, r2_full_no_qc (nrow = ", nrow(df), ")")