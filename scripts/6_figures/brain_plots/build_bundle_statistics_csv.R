#!/usr/bin/env Rscript

# ============================================================
# BUILD BUNDLE STATISTICS CSV
# ============================================================
# Purpose:
#   Create a CSV with one row per bundle (full name = category_bundle, e.g.
#   Association_ArcuateFasciculusL), with columns:
#   - bundle (full name including category)
#   - bundle_category (e.g. Association)
#   - age_effect_no_quality_{metric} (age effect, no QC covariate)
#   - age_effect_with_contrast_{metric} (age effect with t1post_dwi_contrast)
#   - batch_effect_{metric} (pooled harmonized, no quality covariate)
#   - quality_effect_contrast_{metric} (quality effect of t1post_dwi_contrast)
#   for each of the five metrics: DKI_mkt, NODDI_icvf, MAPMRI_rtop, GQI_fa, GQI_md.
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(fs)
  library(jsonlite)
  library(readr)
})

log_info <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n")
}

# --- Config and paths ---
config_candidates <- c(
  Sys.getenv("CONFIG_PATH", unset = ""),
  fs::path(".", "config.json"),
  fs::path("..", "config.json"),
  fs::path("..", "..", "config.json")
)
config_candidates <- normalizePath(unique(config_candidates[nzchar(config_candidates)]), winslash = "/", mustWork = FALSE)
config_path <- config_candidates[file.exists(config_candidates)][1]
if (is.na(config_path) || !nzchar(config_path)) {
  stop("Could not locate config.json.")
}
config <- jsonlite::fromJSON(config_path)
project_root <- normalizePath(config$project_root, winslash = "/", mustWork = FALSE)

metrics_keep <- c("DKI_mkt", "NODDI_icvf", "MAPMRI_rtop", "GQI_fa", "GQI_md")

age_rds    <- fs::path(project_root, "data", "age_effects", "age_effects_all_outputs.rds")
batch_rds  <- fs::path(project_root, "data", "batch_effects", "batch_effects_all_outputs.rds")
quality_rds <- fs::path(project_root, "data", "quality_effects", "quality_effects_all_outputs.rds")

for (f in c(age_rds, batch_rds, quality_rds)) {
  if (!file.exists(f)) {
    stop("Missing file: ", f, "\nRun the corresponding assemble_* script first.")
  }
}

# --- Load data ---
log_info("Loading age effects:", age_rds)
df_age <- readRDS(age_rds)
log_info("Loading batch effects:", batch_rds)
df_batch <- readRDS(batch_rds)
log_info("Loading quality effects:", quality_rds)
df_quality <- readRDS(quality_rds)

# Age: pooled, harmonized
# - no quality: qc_metric == "no_quality" -> age_effect_no_quality_{metric}
# - with contrast: qc_metric == "t1post_dwi_contrast" -> age_effect_with_contrast_{metric}
df_age_pooled <- df_age %>%
  filter(
    output_type == "pooled",
    source == "harmonized",
    metric %in% metrics_keep,
    qc_metric %in% c("no_quality", "t1post_dwi_contrast")
  )

age_no_qc <- df_age_pooled %>%
  filter(qc_metric == "no_quality") %>%
  transmute(
    bundle_category = as.character(bundle_category),
    bundle_short = as.character(bundle),
    metric = as.character(metric),
    age_effect_no_quality = as.numeric(age_effect_size)
  ) %>%
  tidyr::pivot_wider(
    names_from = metric,
    values_from = age_effect_no_quality,
    names_prefix = "age_effect_no_quality_"
  )

age_with_contrast <- df_age_pooled %>%
  filter(qc_metric == "t1post_dwi_contrast") %>%
  transmute(
    bundle_category = as.character(bundle_category),
    bundle_short = as.character(bundle),
    metric = as.character(metric),
    age_effect_with_contrast = as.numeric(age_effect_size)
  ) %>%
  tidyr::pivot_wider(
    names_from = metric,
    values_from = age_effect_with_contrast,
    names_prefix = "age_effect_with_contrast_"
  )

# Batch: pooled (output_type == "batch_effects"), harmonized, no_quality
df_batch_pooled <- df_batch %>%
  filter(
    output_type == "batch_effects",
    source == "raw",
    (is.na(qc_covariate) | qc_covariate == "no_quality"),
    metric %in% metrics_keep
  )

batch_wide <- df_batch_pooled %>%
  transmute(
    bundle_category = as.character(bundle_category),
    bundle_short = as.character(bundle),
    metric = as.character(metric),
    batch_effect = as.numeric(effect_size)
  ) %>%
  tidyr::pivot_wider(
    names_from = metric,
    values_from = batch_effect,
    names_prefix = "batch_effect_"
  )

# Quality: t1post_dwi_contrast only (pooled = non_vendorwise)
df_quality_contrast <- df_quality %>%
  filter(
    qc_metric == "t1post_dwi_contrast",
    metric %in% metrics_keep
  )
if ("output_type" %in% names(df_quality_contrast)) {
  df_quality_contrast <- df_quality_contrast %>%
    filter(output_type == "non_vendorwise_pairwise")
}

quality_wide <- df_quality_contrast %>%
  transmute(
    bundle_category = as.character(bundle_category),
    bundle_short = as.character(bundle),
    metric = as.character(metric),
    quality_effect_contrast = as.numeric(qc_effect_size)
  ) %>%
  tidyr::pivot_wider(
    names_from = metric,
    values_from = quality_effect_contrast,
    names_prefix = "quality_effect_contrast_"
  )

# --- Build full bundle key (category_bundle) and join ---
# Full bundle name = bundle_category_bundle (e.g. Association_ArcuateFasciculusL)
make_full_bundle <- function(cat, short) {
  paste0(cat, "_", short)
}

# Collect all (bundle_category, bundle_short) from any source
keys_age    <- age_no_qc %>% distinct(bundle_category, bundle_short)
keys_batch  <- batch_wide %>% distinct(bundle_category, bundle_short)
keys_qual   <- quality_wide %>% distinct(bundle_category, bundle_short)
keys <- bind_rows(keys_age, keys_batch, keys_qual) %>% distinct(bundle_category, bundle_short)

out <- keys %>%
  mutate(bundle = make_full_bundle(bundle_category, bundle_short)) %>%
  left_join(age_no_qc, by = c("bundle_category", "bundle_short")) %>%
  left_join(age_with_contrast, by = c("bundle_category", "bundle_short")) %>%
  left_join(batch_wide, by = c("bundle_category", "bundle_short")) %>%
  left_join(quality_wide, by = c("bundle_category", "bundle_short"))

# Column order: bundle, bundle_category, then age_no_quality_*, age_with_contrast_*, batch_effect_*, quality_effect_contrast_*
age_no_cols    <- paste0("age_effect_no_quality_", metrics_keep)
age_contrast_cols <- paste0("age_effect_with_contrast_", metrics_keep)
batch_cols     <- paste0("batch_effect_", metrics_keep)
quality_cols   <- paste0("quality_effect_contrast_", metrics_keep)
all_effect_cols <- c(age_no_cols, age_contrast_cols, batch_cols, quality_cols)

# Ensure all expected columns exist (fill with NA if missing from a source)
for (col in all_effect_cols) {
  if (!col %in% names(out)) {
    out[[col]] <- NA_real_
  }
}

out <- out %>%
  select(
    bundle,
    bundle_category,
    all_of(age_no_cols),
    all_of(age_contrast_cols),
    all_of(batch_cols),
    all_of(quality_cols)
  )

out_csv <- fs::path(project_root, "data", "bundle_statistics.csv")
write_csv(out, out_csv)

log_info("Wrote ", nrow(out), " rows to ", out_csv)
