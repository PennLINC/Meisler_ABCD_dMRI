#!/usr/bin/env Rscript

# ============================================================
# ABCD dMRI DATA MERGE PIPELINE
# ============================================================
# Purpose:
#   Merge ABCD demographics, scanner QC, FastTrack QC,
#   AutoTrack bundle geometry, and tract microstructure metrics
#   (masked_mean + masked_median) into a single analysis-ready dataset.
#
# Notes:
#   * Requires CONFIG_PATH environment variable pointing to config.json
#   * Outputs saved to PROJECT_ROOT/data/raw_data/
#   * Intermediate results cached to avoid re-processing
#   * Processes bundle-level metrics from AutoTrack and scalar stats
#     from multiple diffusion models (GQI, DKI, MAPMRI, NODDI)
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
  library(arrow)
  library(fs)
  library(purrr)
})

# ============================================================
# CONFIGURATION
# ============================================================
# Load project-level configuration file that defines:
#   - PROJECT_ROOT: Base directory for this project
#   - LASSO_ROOT: Directory containing ABCD/LASSO data
#   - R_ENV, PYTHON_ENV: Software environment paths

# Check if jsonlite package is available
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

# Load JSON config
config <- jsonlite::fromJSON(config_path)

# Extract variables (convert to R naming convention)
PROJECT_ROOT <- normalizePath(config$project_root, mustWork = FALSE)
LASSO_ROOT <- normalizePath(config$lasso_root, mustWork = FALSE)
FIGURE_DIR <- config$figure_dir
R_ENV <- config$r_env
PYTHON_ENV <- config$python_env

# Define data directory paths
abcc_dir  <- fs::path(LASSO_ROOT, "abcc")   # ABCD Connectome data
dairc_dir <- fs::path(LASSO_ROOT, "dairc")  # DAIRC (FreeSurfer) data

# Define raw data directory (input CSV files and output location)
raw_data_dir <- fs::path(PROJECT_ROOT, "data", "raw_data")

# Define output directory structure (outputs go to raw_data directory)
output_dir      <- raw_data_dir
cache_dir       <- fs::path(output_dir, "cache")              # For cached intermediate results

# Create output directories if they don't exist
dir_create(output_dir, recurse = TRUE)
dir_create(cache_dir)

# Input CSV files from raw_data directory
scanner_qc_file <- fs::path(raw_data_dir, "abcc_0.21.4_scanner_qc.csv")
atk_csv <- fs::path(raw_data_dir, "abcc_atk_sanity_check.csv")

# Input file paths
participants_file <- fs::path(abcc_dir, "rawdata", "participants.tsv")
fasttrack_qc_file <- fs::path(dairc_dir, "sourcedata", "fasttrack_mri_qc.csv")
freesurfer_dir    <- fs::path(dairc_dir, "derivatives", "freesurfer")

# QSIRecon derivative directories
recon_output_dir      <- fs::path(abcc_dir, "derivatives")
autotrack_output_dir <- fs::path(recon_output_dir, "qsirecon-MSMTAutoTrack")

# Final output file path
final_output_file <- fs::path(output_dir, "merged_data.parquet")
# ============================================================
# CONSTANTS
# ============================================================
# Define column names and file patterns used throughout the script

# AutoTrack bundle geometry metrics to extract from bundlestats files
shape_metrics <- c(
  "bundle_name",
  "1st_quarter_volume_mm3",
  "2nd_and_3rd_quarter_volume_mm3",
  "4th_quarter_volume_mm3",
  "area_of_end_region_1_mm2",
  "area_of_end_region_2_mm2",
  "curl",
  "elongation",
  "irregularity",
  "mean_length_mm",
  "number_of_tracts",
  "span_mm",
  "total_volume_mm3"
)

# Scanner QC columns that should be treated as numeric
qc_numeric_cols <- c(
  "raw_dimension_x", "raw_dimension_y", "raw_dimension_z",
  "raw_voxel_size_x", "raw_voxel_size_y", "raw_voxel_size_z",
  "raw_max_b", "raw_neighbor_corr", "raw_masked_neighbor_corr",
  "raw_dwi_contrast", "raw_num_bad_slices", "raw_num_directions",
  "raw_coherence_index", "raw_incoherence_index",
  "t1_dimension_x", "t1_dimension_y", "t1_dimension_z",
  "t1_voxel_size_x", "t1_voxel_size_y", "t1_voxel_size_z",
  "t1_max_b", "t1_neighbor_corr", "t1_masked_neighbor_corr",
  "t1_dwi_contrast", "t1_num_bad_slices", "t1_num_directions",
  "t1_coherence_index", "t1_incoherence_index",
  "t1post_dimension_x", "t1post_dimension_y", "t1post_dimension_z",
  "t1post_voxel_size_x", "t1post_voxel_size_y", "t1post_voxel_size_z",
  "t1post_max_b", "t1post_neighbor_corr", "t1post_masked_neighbor_corr",
  "t1post_dwi_contrast", "t1post_num_bad_slices", "t1post_num_directions",
  "t1post_coherence_index", "t1post_incoherence_index",
  "mean_fd", "max_fd", "max_rotation", "max_translation",
  "max_rel_rotation", "max_rel_translation",
  "t1_dice_distance",
  "CNR0_mean", "CNR1_mean", "CNR2_mean", "CNR3_mean", "CNR4_mean",
  "CNR0_median", "CNR1_median", "CNR2_median", "CNR3_median", "CNR4_median",
  "CNR0_standard_deviation", "CNR1_standard_deviation",
  "CNR2_standard_deviation", "CNR3_standard_deviation", "CNR4_standard_deviation"
)

# QSIRecon pipeline suffixes for different diffusion models
recon_suffixes <- c(
  "qsirecon-DSIStudioGQI",      # Generalized Q-Sampling Imaging
  "qsirecon-DIPYDKI",            # Diffusion Kurtosis Imaging
  "qsirecon-TORTOISE_model-MAPMRI",  # Mean Apparent Propagator MRI
  "qsirecon-wmNODDI"             # Neurite Orientation Dispersion and Density Imaging
)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

# Logging function for progress messages with timestamps
log_info <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n")
}

# Log excluded subjects/sessions for a given exclusion flag
log_exclusions <- function(df, flag_col_name) {
  bad <- df %>% filter(.data[[flag_col_name]] == TRUE)
  if (nrow(bad) > 0) {
    cat("\n--- ", flag_col_name, ": ", nrow(bad), " rows flagged ---\n", sep = "")
    for (i in seq_len(nrow(bad))) {
      cat(
        "Excluded (", flag_col_name, "): ",
        bad$subject_id[i], ", ", bad$session_id[i], "\n", sep = ""
      )
    }
  } else {
    cat("\n--- ", flag_col_name, ": no rows flagged ---\n", sep = "")
  }
}

# Remove NDARINV prefix from subject IDs
clean_subject_id <- function(x) str_replace(x, "^NDARINV", "")

# Normalize session IDs to consistent format
normalize_session_id <- function(x) {
  recode(
    x,
    "ses-baselineYear1Arm1"   = "ses-00A",
    "ses-2YearFollowUpYArm1" = "ses-02A",
    .default = x
  )
}

# Normalize scanner manufacturer names to consistent format
normalize_manufacturer <- function(x) {
  case_when(
    is.na(x) ~ NA_character_,
    str_detect(toupper(x), "SIEMENS")  ~ "Siemens",
    str_detect(toupper(x), "^GE|GENERAL ELECTRIC") ~ "GE",
    str_detect(toupper(x), "PHILIPS")  ~ "Philips",
    TRUE ~ x
  )
}

# Extract subject_session identifier from file paths
# Converts "sub-XXX/ses-YYY" to "sub-XXX_ses-YYY"
extract_subject_session <- function(x) {
  str_extract(x, "sub-[^/]+/ses-[^/]+") %>%
    str_replace_all("/", "_")
}

# Normalize bundle names to include category/bundle separator underscore.
# Example: ProjectionBrainstemCorticobulbarTractL -> ProjectionBrainstem_CorticobulbarTractL
normalize_bundle_name <- function(x) {
  bundle_name <- as.character(x)
  category_prefixes <- c(
    "ProjectionBrainstem",
    "ProjectionBasalGanglia",
    "Commissure",
    "Association",
    "Cerebellum"
  )

  for (prefix in category_prefixes) {
    bundle_name <- if_else(
      is.na(bundle_name) | str_detect(bundle_name, "_"),
      bundle_name,
      str_replace(bundle_name, paste0("^", prefix), paste0(prefix, "_"))
    )
  }

  bundle_name
}

# ------------------------------------------------------------
# FreeSurfer eTIV (Estimated Total Intracranial Volume) reader
# Reads eTIV value from aseg.stats file
# ------------------------------------------------------------
read_etiv <- function(subject_session) {
  f <- fs::path(
    freesurfer_dir,
    subject_session,
    "stats",
    "aseg.stats"
  )

  if (!file_exists(f)) return(NA_real_)

  line <- read_lines(f, skip = 33, n_max = 1)
  if (length(line) == 0) return(NA_real_)

  as.numeric(
    gsub(
      ",", "",
      str_extract(line, "[0-9,]+\\.[0-9]+|[0-9,]+$")
    )
  )
}

# ============================================================
# SECTION A: SESSION-LEVEL METADATA (CACHED)
# ============================================================
# Load session-level metadata from ABCD Connectome data.
# This includes scanner information, session identifiers, etc.
# Results are cached to avoid re-reading on subsequent runs.

log_info("Loading session tables")

sessions_cache_file <- fs::path(cache_dir, "sessions_df.rds")
log_info("Session cache path:", sessions_cache_file)

if (file_exists(sessions_cache_file)) {
  # Load from cache if available
  log_info("Cache hit: loading sessions from RDS")
  all_sessions <- readRDS(sessions_cache_file)
} else {
  log_info("Cache miss: scanning rawdata for *_sessions.tsv files")
  # Find all session TSV files in rawdata directory
  session_files <- dir_ls(
    fs::path(abcc_dir, "rawdata"),
    recurse = TRUE,
    type = "file",
    regexp = "_sessions\\.tsv$"
  )

  # Read and combine all session files
  all_sessions <- map_dfr(session_files, function(f) {
    # Extract subject ID from filename
    subject_id <- str_remove(basename(f), "_sessions.tsv")

    read_tsv(
      f,
      col_types = cols(.default = col_character()),
      show_col_types = FALSE
    ) %>%
      # Remove connectivity and cortical/subcortical columns
      select(
        -matches("pconn|bc_|#subcortical|#cortical"),
        -any_of(c("task_id", "dir_id", "acq_id", "space_id", "rec_id", "run_id"))
      ) %>%
      mutate(
        subject_id = subject_id,
        session_id = normalize_session_id(session_id),
        scanner_manufacturer = normalize_manufacturer(scanner_manufacturer)
      )
  })

  # Save to cache for future runs
  saveRDS(all_sessions, sessions_cache_file)
  log_info("Saved session cache:", sessions_cache_file, "rows:", nrow(all_sessions))
}

# ============================================================
# SECTION B: DERIVED METADATA (FreeSurfer eTIV, cached)
# ============================================================
# Extract eTIV (Estimated Total Intracranial Volume) from FreeSurfer
# aseg.stats files. This is used as a covariate in analyses.
# Results are cached to avoid re-reading on subsequent runs.

log_info("Indexing FreeSurfer eTIV")

etiv_cache_file <- fs::path(cache_dir, "etiv_lookup.rds")

if (file_exists(etiv_cache_file)) {
  # Load from cache if available
  etiv_df <- readRDS(etiv_cache_file)
} else {
  # Find all aseg.stats files
  aseg_files <- dir_ls(
    freesurfer_dir,
    recurse = TRUE,
    regexp = "aseg\\.stats$"
  )

  # Extract eTIV from each file
  etiv_df <- map_dfr(aseg_files, function(f) {
    # Extract subject_session from directory structure
    subject_session <- basename(dirname(dirname(f)))

    # Read eTIV line (line 34, after 33 header lines)
    line <- read_lines(f, skip = 33, n_max = 1)

    eTIV <- if (length(line) == 0) {
      NA_real_
    } else {
      as.numeric(
        gsub(
          ",", "",
          str_extract(line, "[0-9,]+\\.[0-9]+|[0-9,]+$")
        )
      )
    }

    tibble(
      subject_session = subject_session,
      eTIV = eTIV
    )
  })

  # Save to cache for future runs
  saveRDS(etiv_df, etiv_cache_file)
}

# ============================================================
# SECTION C: METADATA + QC MERGING
# ============================================================
# Combine session metadata with participant demographics,
# scanner QC metrics, AutoTrack QC, and FastTrack QC.
# Create initial exclusion flags based on missing data.

# Load participant demographics
participants_df <- read_tsv(participants_file, show_col_types = FALSE) %>%
  rename(subject_id = participant_id)

# Load scanner QC metrics
qc_df <- read_csv(scanner_qc_file, show_col_types = FALSE) %>%
  mutate(
    subject_id = clean_subject_id(subject_id),
    session_id = normalize_session_id(session_id)
  ) %>%
  select(
    subject_id,
    session_id,
    DeviceSerialNumber,
    any_of(qc_numeric_cols)
  ) %>%
  # Handle duplicate rows by keeping first occurrence
  group_by(subject_id, session_id) %>%
  slice(1) %>%
  ungroup()

# Load AutoTrack QC flags
atk_df <- read_csv(atk_csv, show_col_types = FALSE)

# Load FastTrack QC: dMRI only, conservative collapse across raters
# A session is usable only if ALL raters mark it as usable
fasttrack_qc_df <- read_csv(fasttrack_qc_file, show_col_types = FALSE) %>%
  filter(type == "ABCD-DTI") %>%
  mutate(
    subject_id = clean_subject_id(participant_id),
    session_id = normalize_session_id(session_id),
    usable = as.numeric(usable)
  ) %>%
  group_by(subject_id, session_id) %>%
  summarise(
    fasttrack_usable = all(usable == 1),
    .groups = "drop"
  )

# Merge all metadata and QC sources
merged_df <- all_sessions %>%
  left_join(participants_df, by = "subject_id") %>%
  left_join(qc_df, by = c("subject_id", "session_id")) %>%
  left_join(atk_df, by = c("subject_id", "session_id")) %>%
  left_join(fasttrack_qc_df, by = c("subject_id", "session_id")) %>%
  mutate(
    subject_session = paste0(subject_id, "_", session_id),
    site = str_replace(as.character(site), "\\.0$", ""),
    # Exclusion flags: missing QSIPrep QC or FastTrack QC failure
    no_qsiprep_exclude = is.na(t1_neighbor_corr),
    fasttrack_exclude = !fasttrack_usable
  ) %>%
  distinct(subject_session, .keep_all = TRUE)

# Apply site-specific exclusion criteria
# ---- site_888_exclude: Exclude test site 888 ----
merged_df <- merged_df %>%
  mutate(
    site_888_exclude = str_detect(site, "888")
  )
log_exclusions(merged_df, "site_888_exclude")

# ---- scanner_manufacturer_888_exclude: Exclude scanner manufacturer containing 888 ----
merged_df <- merged_df %>%
  mutate(
    scanner_manufacturer_888_exclude = str_detect(scanner_manufacturer, "888")
  )
log_exclusions(merged_df, "scanner_manufacturer_888_exclude")

# ---- missing_device_serial_exclude: Exclude missing or empty device serial numbers ----
merged_df <- merged_df %>%
  mutate(
    missing_device_serial_exclude = is.na(DeviceSerialNumber) | DeviceSerialNumber == ""
  )
log_exclusions(merged_df, "missing_device_serial_exclude")

# ---- site_22_exclude: Exclude site 22 ----
merged_df <- merged_df %>%
  mutate(
    site_22_exclude = site == "22"
  )
log_exclusions(merged_df, "site_22_exclude")

# ---- bad_site_manufacturer_combo_exclude: Known problematic combinations ----
merged_df <- merged_df %>%
  mutate(
    bad_site_manufacturer_combo_exclude =
      (site == "6" & scanner_manufacturer == "Philips") |
      (site == "9" & scanner_manufacturer == "GE")
  )
log_exclusions(merged_df, "bad_site_manufacturer_combo_exclude")

# ============================================================
# SECTION D: FILE DISCOVERY (CACHED)
# ============================================================
# Discover all bundle statistics and scalar statistics files
# from QSIRecon outputs. Results are cached to avoid re-scanning
# the filesystem on subsequent runs.

bundle_cache_file <- fs::path(cache_dir, "bundle_files.rds")
scalar_cache_file <- fs::path(cache_dir, "scalar_files.rds")

if (file_exists(bundle_cache_file) && file_exists(scalar_cache_file)) {
  # Load from cache if available
  bundle_files <- readRDS(bundle_cache_file)
  scalar_files <- readRDS(scalar_cache_file)
} else {
  # Find all AutoTrack bundle statistics files
  bundle_files <- Sys.glob(
    file.path(autotrack_output_dir, "sub-*", "ses-*", "dwi", "*bundlestats*.csv")
  )

  # Find all scalar statistics files across all QSIRecon models
  scalar_files <- unlist(lapply(recon_suffixes, function(suf) {
    Sys.glob(file.path(
      recon_output_dir, suf, "sub-*", "ses-*", "dwi", "*_scalarstats.tsv"
    ))
  }))

  # Save to cache for future runs
  saveRDS(bundle_files, bundle_cache_file)
  saveRDS(scalar_files, scalar_cache_file)
}

# Derive QSIRecon presence flags (SESSION LEVEL)
# Check which QSIRecon components are available for each session
log_info("Deriving QSIRecon presence flags")

# ------------------------------------------------------------
# Bundle stats presence (AutoTrack)
# ------------------------------------------------------------
bundlestats_sessions <- tibble(
  subject_session = extract_subject_session(bundle_files),
  has_bundlestats = TRUE
) %>%
  filter(!is.na(subject_session)) %>%
  distinct()

# ------------------------------------------------------------
# Scalar stats presence by diffusion model
# ------------------------------------------------------------
scalar_sessions <- tibble(
  subject_session = extract_subject_session(scalar_files),
  file = scalar_files
) %>%
  filter(!is.na(subject_session)) %>%
  mutate(
    has_GQI     = str_detect(file, "DSIStudioGQI"),
    has_DKI     = str_detect(file, "DIPYDKI"),
    has_MAPMRI  = str_detect(file, "MAPMRI"),
    has_NODDI   = str_detect(file, "wmNODDI")
  ) %>%
  group_by(subject_session) %>%
  summarise(
    has_GQI    = any(has_GQI),
    has_DKI    = any(has_DKI),
    has_MAPMRI = any(has_MAPMRI),
    has_NODDI  = any(has_NODDI),
    .groups = "drop"
  )

# ------------------------------------------------------------
# Join presence flags to session table and define exclusions
# ------------------------------------------------------------
merged_df <- merged_df %>%
  left_join(bundlestats_sessions, by = "subject_session") %>%
  left_join(scalar_sessions, by = "subject_session") %>%
  mutate(
    # Replace missing presence flags with FALSE
    across(
      c(has_bundlestats, has_GQI, has_DKI, has_MAPMRI, has_NODDI),
      ~ if_else(is.na(.x), FALSE, .x)
    ),

    # Component-level exclusion flags
    no_bundlestats_exclude = !has_bundlestats,
    no_GQI_exclude         = !has_GQI,
    no_DKI_exclude         = !has_DKI,
    no_MAPMRI_exclude      = !has_MAPMRI,
    no_NODDI_exclude       = !has_NODDI,

    # Incomplete QSIRecon: missing ANY required component
    incomplete_qsirecon_exclude =
      no_bundlestats_exclude |
      no_GQI_exclude |
      no_DKI_exclude |
      no_MAPMRI_exclude |
      no_NODDI_exclude
  )

log_exclusions(merged_df, "incomplete_qsirecon_exclude")

# Extract list of subject_session identifiers for downstream processing
# (extracted after QSIRecon presence flags are added)
subject_sessions <- merged_df$subject_session

# ============================================================
# SECTION E: IN-MEMORY SUBJECT-LEVEL PROCESSING
# ============================================================
# Process bundle geometry and microstructure metrics for each subject.
# This section:
#   1. Reads AutoTrack bundle statistics (geometry metrics)
#   2. Reads scalar statistics (microstructure metrics) from all models
#   3. Reshapes data into wide format with bundle-specific column names
#   4. Combines all metrics per subject_session
# Results are cached to avoid re-processing on subsequent runs.

bundle_stats_cache_file <- fs::path(cache_dir, "bundle_stats_df.rds")

if (file_exists(bundle_stats_cache_file)) {
  log_info("Loading cached bundle_stats_df")
  bundle_stats_df <- readRDS(bundle_stats_cache_file)
} else {
  log_info("Processing subject bundle statistics (in memory)")

  # Initialize list to store processed data for each subject_session
  bundle_stats_list <- vector("list", length(subject_sessions))
  names(bundle_stats_list) <- subject_sessions

  # Process each subject_session
  for (i in seq_along(subject_sessions)) {
    ss <- subject_sessions[i]
    
    log_info(paste("Processing ", ss, " (", i, "/", length(subject_sessions), ")\n", sep = ""))

    # Find all bundle and scalar files for this subject_session
    files <- c(
      bundle_files[str_detect(bundle_files, ss)],
      scalar_files[str_detect(scalar_files, ss)]
    )

    if (length(files) == 0) next

    # List to store processed dataframes for this subject
    out <- list()

    for (f in files) {
      # --------------------------------------------------------
      # Process AutoTrack bundle statistics
      # Extract geometry metrics (volume, length, etc.) per bundle
      # --------------------------------------------------------
      if (str_detect(f, "bundlestats")) {
        df <- read_csv(f, show_col_types = FALSE)

        # Process each bundle separately
        df %>%
          select(any_of(shape_metrics)) %>%
          group_split(bundle_name) %>%
          walk(function(subdf) {
            bundle_name <- normalize_bundle_name(unique(subdf$bundle_name))
            subdf <- select(subdf, -bundle_name)

            # Create bundle-specific column names
            names(subdf) <- paste0(
              "bundle_", bundle_name, "_", names(subdf)
            )

            # Store as separate dataframe in list
            out[[length(out) + 1]] <<-
              tibble(subject_session = ss) %>%
              bind_cols(subdf)
          })
      }

      # --------------------------------------------------------
      # Process scalar statistics (masked mean + median)
      # Extract microstructure metrics per bundle and model
      # --------------------------------------------------------
      if (str_detect(f, "scalarstats")) {
        # Identify diffusion model from file path
        model <- case_when(
          str_detect(f, "wmNODDI")        ~ "NODDI",
          str_detect(f, "DIPYDKI")        ~ "DKI",
          str_detect(f, "MAPMRI")         ~ "MAPMRI",
          str_detect(f, "DSIStudioGQI")   ~ "GQI",
          TRUE ~ "UNKNOWN"
        )

        df <- read_tsv(f, show_col_types = FALSE) %>%
          select(bundle, variable_name, masked_mean, masked_median) %>%
          mutate(
            bundle = normalize_bundle_name(bundle),
            # Remove duplicated metric prefixes (e.g., dki_, dti_) regardless of case
            # so columns become DKI_ad (not DKI_dki_ad) and GQI_fa (not GQI_dti_fa).
            variable_name = str_replace(
              variable_name,
              regex("^(dki_|dti_)+", ignore_case = TRUE),
              ""
            ),
            # Create column names for mean and median
            f_mean   = paste0("bundle_", bundle, "_", model, "_", variable_name, "_mean"),
            f_median = paste0("bundle_", bundle, "_", model, "_", variable_name, "_median")
          )

        # Reshape mean values to wide format
        mean_wide <- df %>%
          select(f_mean, masked_mean) %>%
          pivot_wider(
            names_from  = f_mean,
            values_from = masked_mean
          )

        # Reshape median values to wide format
        median_wide <- df %>%
          select(f_median, masked_median) %>%
          pivot_wider(
            names_from  = f_median,
            values_from = masked_median
          )

        # Combine mean and median into single dataframe
        out[[length(out) + 1]] <-
          tibble(subject_session = ss) %>%
          bind_cols(mean_wide, median_wide)
      }
    }

    # Combine all processed dataframes for this subject
    if (length(out) > 0) {
      bundle_stats_list[[i]] <-
        reduce(out, full_join, by = "subject_session")
    }
  }

  # ------------------------------------------------------------
  # Combine all subjects into one dataframe
  # ------------------------------------------------------------
  bundle_stats_df <- bind_rows(bundle_stats_list)

  log_info("Finished processing bundle statistics")

  # Save to cache for future runs
  saveRDS(bundle_stats_df, bundle_stats_cache_file)

  log_info(
    "Cached bundle_stats_df:",
    nrow(bundle_stats_df), "rows,",
    ncol(bundle_stats_df), "columns"
  )
}

# Safety: ensure all bundle_stats_df feature columns keep the "bundle_" prefix.
# (This also corrects any legacy cached names missing the prefix.)
bundle_feature_cols <- setdiff(names(bundle_stats_df), "subject_session")
cols_missing_prefix <- bundle_feature_cols[!str_starts(bundle_feature_cols, "bundle_")]
if (length(cols_missing_prefix) > 0) {
  log_info("Adding missing bundle_ prefix to", length(cols_missing_prefix), "columns")
  bundle_stats_df <- bundle_stats_df %>%
    rename_with(
      .cols = any_of(cols_missing_prefix),
      .fn = ~ paste0("bundle_", .x)
    )
}

# Safety: normalize legacy bundle naming in cached columns so category and bundle
# are separated by an underscore (e.g., ProjectionBrainstemCST -> ProjectionBrainstem_CST).
bundle_cols_after_prefix <- setdiff(names(bundle_stats_df), "subject_session")
bundle_cols_after_prefix <- bundle_cols_after_prefix[str_starts(bundle_cols_after_prefix, "bundle_")]

normalize_bundle_colname <- function(col_name) {
  col_name %>%
    # Repair incorrect legacy names that split valid projection categories.
    str_replace("^bundle_Projection_Brainstem", "bundle_ProjectionBrainstem") %>%
    str_replace("^bundle_Projection_BasalGanglia", "bundle_ProjectionBasalGanglia") %>%
    # Add separator only for complete category names.
    str_replace("^bundle_ProjectionBrainstem(?=[A-Z])", "bundle_ProjectionBrainstem_") %>%
    str_replace("^bundle_ProjectionBasalGanglia(?=[A-Z])", "bundle_ProjectionBasalGanglia_") %>%
    str_replace("^bundle_Commissure(?=[A-Z])", "bundle_Commissure_") %>%
    str_replace("^bundle_Association(?=[A-Z])", "bundle_Association_") %>%
    str_replace("^bundle_Cerebellum(?=[A-Z])", "bundle_Cerebellum_")
}

normalized_bundle_cols <- vapply(bundle_cols_after_prefix, normalize_bundle_colname, character(1))
cols_to_normalize <- bundle_cols_after_prefix[bundle_cols_after_prefix != normalized_bundle_cols]

if (length(cols_to_normalize) > 0) {
  # dplyr::rename expects new_name = old_name
  rename_map <- stats::setNames(
    cols_to_normalize,
    normalized_bundle_cols[bundle_cols_after_prefix != normalized_bundle_cols]
  )
  log_info("Normalizing legacy bundle naming in", length(cols_to_normalize), "cached columns")
  bundle_stats_df <- bundle_stats_df %>% rename(!!!rename_map)
}

# Safety: normalize cached scalar metric names that contain duplicated middle
# prefixes (e.g., _DKI_dki_*, _GQI_dti_*) from older runs.
bundle_feature_cols_after_norm <- setdiff(names(bundle_stats_df), "subject_session")
cleaned_feature_cols <- bundle_feature_cols_after_norm %>%
  str_replace(regex("_DKI_dki_", ignore_case = TRUE), "_DKI_") %>%
  str_replace(regex("_DKI_dti_", ignore_case = TRUE), "_DKI_") %>%
  str_replace(regex("_GQI_dki_", ignore_case = TRUE), "_GQI_") %>%
  str_replace(regex("_GQI_dti_", ignore_case = TRUE), "_GQI_")

cols_with_duplicate_metric_prefix <- bundle_feature_cols_after_norm[
  bundle_feature_cols_after_norm != cleaned_feature_cols
]

if (length(cols_with_duplicate_metric_prefix) > 0) {
  rename_map_dup <- stats::setNames(
    cols_with_duplicate_metric_prefix,
    cleaned_feature_cols[bundle_feature_cols_after_norm != cleaned_feature_cols]
  )
  log_info(
    "Normalizing duplicated scalar metric prefixes in",
    length(cols_with_duplicate_metric_prefix),
    "cached columns"
  )
  bundle_stats_df <- bundle_stats_df %>% rename(!!!rename_map_dup)
}

# ============================================================
# SECTION F: FINAL ASSEMBLY
# ============================================================
# Combine all data sources into final merged dataset:
#   - Session metadata and demographics
#   - QC flags and exclusion criteria
#   - Bundle geometry statistics (AutoTrack)
#   - Tract microstructure metrics (GQI, DKI, MAPMRI, NODDI)
#   - FreeSurfer eTIV (estimated Total Intracranial Volume)

merged_all <- merged_df %>%
  left_join(bundle_stats_df, by = "subject_session") %>%
  left_join(etiv_df, by = "subject_session")

# Drop requested metrics before typing/saving.
excluded_metric_tokens <- c(
  "NODDI_directions",
  "GQI_ha",
  "GQI_txx",
  "GQI_txy",
  "GQI_txz",
  "GQI_tyy",
  "GQI_tyz",
  "GQI_tzz"
)

excluded_metric_pattern <- paste0("_(?:", str_c(excluded_metric_tokens, collapse = "|"), ")_")
excluded_metric_cols <- names(merged_all)[
  str_detect(names(merged_all), excluded_metric_pattern)
]

if (length(excluded_metric_cols) > 0) {
  log_info("Dropping excluded metric columns:", length(excluded_metric_cols))
  merged_all <- merged_all %>% select(-any_of(excluded_metric_cols))
}

# ============================================================
# SECTION G: EXCLUSIONS + NUMERIC TYPING
# ============================================================
# Finalize exclusion flags and ensure proper data types
# for all numeric columns before saving.

# List of all exclusion flag columns
exclusion_cols <- c(
  "atk_exclude",
  "fasttrack_exclude",
  "no_qsiprep_exclude",
  "site_888_exclude",
  "scanner_manufacturer_888_exclude",
  "missing_device_serial_exclude",
  "site_22_exclude",
  "bad_site_manufacturer_combo_exclude",
  "incomplete_qsirecon_exclude"
)

# Normalize exclusion flags to logical and create summary flag
merged_all <- merged_all %>%
  mutate(
    # Ensure all exclusion flags are logical (TRUE/FALSE)
    across(
      any_of(exclusion_cols),
      ~ .x %in% c(TRUE, 1, "1", "TRUE")
    ),
    # Create summary flag: TRUE if NOT excluded by any criterion
    not_excluded = !Reduce(`|`, across(any_of(exclusion_cols)))
  )

# Identify bundle statistic columns for numeric conversion
bundle_stat_cols <- names(merged_all)[
  str_starts(names(merged_all), "bundle_")
]

# Apply explicit numeric typing to all numeric columns
merged_all <- merged_all %>%
  mutate(
    age  = as.numeric(age),
    eTIV = as.numeric(eTIV)
  ) %>%
  mutate(across(any_of(qc_numeric_cols), as.numeric)) %>%
  mutate(across(any_of(bundle_stat_cols), as.numeric)) %>%
  # Move subject_session to first column
  relocate(subject_session, .before = 1)

# Save final merged dataset
write_parquet(merged_all, final_output_file)
log_info("FINAL SAVED:", final_output_file)
