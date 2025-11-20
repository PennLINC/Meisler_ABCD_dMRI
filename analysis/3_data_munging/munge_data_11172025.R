#!/usr/bin/env Rscript

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
base_dir <- "/cbica/projects/abcd_qsiprep/meisler_abcd_paper/munge_data"
demographics_dir <- fs::path(base_dir, "demographics")
output_dir <- fs::path(base_dir, "outputs_11172025")

recon_output_dir <- "/cbica/projects/abcd_qsiprep/recon_results/derivatives"
autotrack_output_dir <- fs::path(recon_output_dir, "qsirecon-MSMTAutoTrack")

whole_brain_measures_path <- "/cbica/projects/abcd_qsiprep/sisk_myelin_dev/analysis/all_mean_values_merged.parquet"
atk_csv <- fs::path(base_dir, "abcc_atk_sanity_check.csv")
vol_csv <- "/cbica/projects/abcd_qsiprep/bmacedo/outputs/all_brain_volumes.csv"

participants_file <- fs::path(demographics_dir, "participants.tsv")
qc_file <- fs::path(demographics_dir, "abcc_0.21.4_scanner_qc.csv")

merged_demos_qc_file <- fs::path(output_dir, "merged_demos_qc.parquet")
final_bundle_stats_file <- fs::path(output_dir, "bundle_stats.parquet")
final_output_file <- fs::path(output_dir, "merged_data_11172025.parquet")

dir_create(output_dir, recurse = TRUE)

shape_metrics <- c(
  "bundle_name",
  "1st_quarter_volume_mm3","2nd_and_3rd_quarter_volume_mm3",
  "4th_quarter_volume_mm3",
  "area_of_end_region_1_mm2","area_of_end_region_2_mm2",
  "curl","elongation","irregularity","mean_length_mm","number_of_tracts",
  "radius_of_end_region_1_mm","radius_of_end_region_2_mm",
  "span_mm","total_area_of_end_regions_mm2",
  "total_radius_of_end_regions_mm",
  "total_surface_area_mm2","total_volume_mm3",
  "volume_of_end_branches_1","volume_of_end_branches_2"
)

recon_suffixes <- c(
  "qsirecon-DSIStudioGQI",
  "qsirecon-DIPYDKI",
  "qsirecon-TORTOISE_model-MAPMRI",
  "qsirecon-wmNODDI"
)

# ============================================================
# LOGGING HELPERS
# ============================================================
log_info <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n")
}

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

# ============================================================
# NORMALIZERS
# ============================================================
clean_subject_id <- function(x) str_replace(x, "^NDARINV", "")

normalize_session_id <- function(x) {
  recode(
    x,
    "ses-baselineYear1Arm1" = "ses-00A",
    "ses-2YearFollowUpYArm1" = "ses-02A",
    .default = x
  )
}

normalize_manufacturer <- function(x) {
  case_when(
    str_detect(x, "SIEMENS") ~ "Siemens",
    str_detect(x, "GE")      ~ "GE",
    str_detect(x, "Philips") ~ "Philips",
    TRUE ~ x
  )
}

# ============================================================
# INFER SITE FROM SERIAL
# ============================================================
infer_site_from_serial <- function(df) {

  if (!"DeviceSerialNumber" %in% names(df)) {
    log_info("WARNING: DeviceSerialNumber missing; cannot infer site.")
    return(df)
  }

  df <- df %>% mutate(site = as.character(site))

  serial_tab <- df %>%
    filter(!is.na(DeviceSerialNumber), DeviceSerialNumber != "") %>%
    count(DeviceSerialNumber, site, name = "n")

  serial_unique <- serial_tab %>%
    group_by(DeviceSerialNumber) %>%
    filter(n == max(n)) %>% slice(1) %>% ungroup()

  mask_bad <- is.na(df$site) | df$site == "" | str_detect(df$site, "888")

  changes <- list()

  for (i in which(mask_bad)) {
    serial <- df$DeviceSerialNumber[i]
    if (!is.na(serial) && serial != "") {
      inferred <- serial_unique %>% filter(DeviceSerialNumber == serial) %>% pull(site)
      if (length(inferred) == 1 && !is.na(inferred)) {

        changes[[length(changes)+1]] <- list(
          subject_id = df$subject_id[i],
          session_id = df$session_id[i],
          subject_session = df$subject_session[i],
          old_site = df$site[i],
          new_site = inferred
        )

        df$site[i] <- inferred
      }
    }
  }

  if (length(changes) > 0) {
    cat("\n--- infer_site_from_serial: site corrections ---\n")
    for (chg in changes) {
      cat(
        "Corrected site for ",
        chg$subject_session,
        " (", chg$subject_id, ", ", chg$session_id, "): ",
        chg$old_site, " -> ", chg$new_site, "\n", sep = ""
      )
    }
    cat("---------------------------------------------\n\n")
  } else {
    cat("\n--- infer_site_from_serial: no site corrections ---\n\n")
  }

  df
}

# ============================================================
# FIX scanner_manufacturer == "888"
# ============================================================
fix_scanner_888 <- function(df) {

  df <- df %>% mutate(scanner_manufacturer = as.character(scanner_manufacturer))
  sites <- unique(df$site)
  changes <- list()

  for (s in sites) {
    idx <- which(df$site == s)
    manufs <- df$scanner_manufacturer[idx]

    if ("888" %in% manufs) {

      inferred <- manufs[manufs != "888" & !is.na(manufs)][1]

      if (!is.na(inferred)) {
        changed_rows <- idx[manufs == "888"]

        for (row_idx in changed_rows) {
          changes[[length(changes)+1]] <- list(
            subject_id = df$subject_id[row_idx],
            session_id = df$session_id[row_idx],
            subject_session = df$subject_session[row_idx],
            old_value = "888",
            new_value = inferred
          )
        }

        df$scanner_manufacturer[changed_rows] <- inferred
      }
    }
  }

  if (length(changes) > 0) {
    cat("\n--- scanner_manufacturer 888 corrections ---\n")
    for (chg in changes) {
      cat(
        "Fixed scanner_manufacturer for ",
        chg$subject_session,
        " (", chg$subject_id, ", ", chg$session_id, "): ",
        chg$old_value, " -> ", chg$new_value, "\n", sep = ""
      )
    }
    cat("--------------------------------------------\n\n")
  } else {
    cat("\n--- No scanner_manufacturer 888 fixes found ---\n\n")
  }

  df
}

# ============================================================
# STEP 1: LOAD DEMOGRAPHICS + QC + ATK
# ============================================================
log_info("STEP 1: Loading demographics, QC, ATK")

participants_df <- read_tsv(participants_file, show_col_types = FALSE) %>%
  rename(subject_id = participant_id)

qc_df <- read_csv(qc_file, show_col_types = FALSE) %>%
  mutate(
    subject_id = clean_subject_id(subject_id),
    session_id = normalize_session_id(session_id),
    t1_neighbor_corr = suppressWarnings(as.numeric(t1_neighbor_corr))
  ) %>%
  group_by(subject_id, session_id) %>% slice(1) %>% ungroup()

atk_df <- read_csv(atk_csv, show_col_types = FALSE) %>%
  mutate(
    subject_id = as.character(subject_id),
    session_id = as.character(session_id)
  )

session_files <- dir_ls(demographics_dir, regexp = "_session.*tsv")
all_sessions <- map_dfr(session_files, function(f) {
  sid <- str_extract(basename(f), "sub-[^_]+")
  read_tsv(f, col_types = cols(.default = "c"), show_col_types = FALSE) %>%
    mutate(subject_id = sid)
})

merged_df <- all_sessions %>%
  left_join(participants_df, by = "subject_id") %>%
  left_join(qc_df, by = c("subject_id", "session_id")) %>%
  left_join(atk_df, by = c("subject_id", "session_id")) %>%
  mutate(
    subject_session = paste0(subject_id, "_", session_id),
    site = str_replace(as.character(site), "\\.0$", "")
  ) %>%
  distinct(subject_session, .keep_all = TRUE)

# QC missing => exclude
merged_df <- merged_df %>%
  mutate(no_qsiprep_exclude = is.na(t1_neighbor_corr) | is.nan(t1_neighbor_corr))
log_exclusions(merged_df, "no_qsiprep_exclude")

log_exclusions(merged_df, "atk_exclude")

merged_df <- infer_site_from_serial(merged_df)

merged_df <- merged_df %>%
  mutate(site_888_exclude =
           str_detect(site, "888") &
           (is.na(DeviceSerialNumber) | DeviceSerialNumber == ""))

log_exclusions(merged_df, "site_888_exclude")

merged_df <- merged_df %>% mutate(site_22_exclude = site == "22")
log_exclusions(merged_df, "site_22_exclude")

merged_df <- fix_scanner_888(merged_df)

merged_df <- merged_df %>%
  mutate(scanner_manufacturer = normalize_manufacturer(scanner_manufacturer))

merged_df <- merged_df %>%
  mutate(
    bad_site_manufacturer_combo_exclude =
      (site == "6" & scanner_manufacturer == "Philips") |
      (site == "9" & scanner_manufacturer == "GE")
  )

log_exclusions(merged_df, "bad_site_manufacturer_combo_exclude")

valid_sessions <- unique(merged_df$subject_session)
log_info("Total sessions: ", length(valid_sessions))

# ============================================================
# STEP 2: FILE LISTS
# ============================================================
log_info("STEP 2: Gathering bundlestats and scalarstats file lists")

bundle_list_file  <- fs::path(output_dir, "bundle_stats_files.txt")
scalar_list_file  <- fs::path(output_dir, "scalar_stats_files.txt")
bundle_stats_checkpoint <- fs::path(output_dir, "bundle_stats_checkpoint.parquet")

if (file_exists(bundle_list_file) && file_exists(scalar_list_file)) {

  bundle_stats_files <- read_lines(bundle_list_file)
  scalar_stats_files <- read_lines(scalar_list_file)

  cat("\nLoaded precomputed file lists:\n")
  cat("  Bundlestats: ", length(bundle_stats_files), "\n", sep = "")
  cat("  Scalarstats: ", length(scalar_stats_files), "\n\n", sep = "")

} else {

  bundle_stats_files <- Sys.glob(file.path(
    autotrack_output_dir, "sub-*", "ses-*", "dwi", "*bundlestats*.csv"
  ))

  scalar_stats_files <- c()
  for (suffix in recon_suffixes) {
    scalar_stats_files <- c(
      scalar_stats_files,
      Sys.glob(file.path(
        fs::path(recon_output_dir, suffix),
        "sub-*", "ses-*", "dwi",
        "*_space-T1w_bundles-MSMTAutoTrack_scalarstats.tsv"
      ))
    )
  }

  write_lines(bundle_stats_files, bundle_list_file)
  write_lines(scalar_stats_files, scalar_list_file)

  cat("\nSaved file lists to ", output_dir, "\n\n", sep = "")
}

bundle_stats_files <- sort(bundle_stats_files)
scalar_stats_files <- sort(scalar_stats_files)

# ============================================================
# DEFINE process_subject()
# ============================================================
process_subject <- function(ss) {

  files <- c(
    bundle_stats_files[str_detect(basename(bundle_stats_files), ss)],
    scalar_stats_files[str_detect(basename(scalar_stats_files), ss)]
  )
  if (length(files) == 0) return(NULL)

  out <- list()

  for (f in files) {
    fname <- basename(f)

    if (str_detect(fname, "bundlestats")) {

      df <- suppressMessages(read_csv(f, show_col_types = FALSE))
      if (!"bundle_name" %in% names(df) || nrow(df) == 0) next

      workflow <- str_extract(fname, "model-[^_]+") %>% str_replace("model-", "")
      if (is.na(workflow)) workflow <- "unknown"

      df_list <- df %>%
        select(any_of(shape_metrics)) %>%
        group_split(bundle_name)

      entries <- map(df_list, function(subdf) {
        bundle <- unique(subdf$bundle_name)
        subdf <- select(subdf, -bundle_name)
        colnames(subdf) <- paste0(
          workflow, "_", str_remove_all(bundle, "_"), "_", colnames(subdf)
        )
        tibble(subject_session = ss) %>% bind_cols(subdf)
      })

      out <- append(out, entries)

    } else if (str_detect(fname, "scalarstats")) {

      df <- suppressMessages(read_tsv(f, show_col_types = FALSE))
      if (nrow(df) == 0) next

      recon <- case_when(
        str_detect(f, "MAPMRI") ~ "MAPMRI",
        str_detect(f, "DKI") ~ "DKI",
        str_detect(f, "NODDI") ~ "NODDI",
        str_detect(f, "GQI") ~ "GQI",
        TRUE ~ "unknown"
      )

      df <- df %>%
        mutate(feature = paste0(
          "msmt_", str_remove_all(bundle, "_"), "_",
          recon, "_", variable_name
        )) %>%
        select(feature, masked_mean)

      wide <- pivot_wider(df, names_from = feature, values_from = masked_mean)

      out <- append(out, list(
        tibble(subject_session = ss) %>% bind_cols(wide)
      ))
    }
  }

  if (length(out) == 0) return(NULL)

  reduce(out, full_join, by = "subject_session")
}

# ============================================================
# STEP 3: MUNGING WITH CHECKPOINT RESUME
# ============================================================
log_info("STEP 3: Merging per-subject bundle and scalar files")

if (file_exists(bundle_stats_checkpoint)) {
  bundle_stats_df <- read_parquet(bundle_stats_checkpoint)
  processed_subject_sessions <- unique(bundle_stats_df$subject_session)
  cat("Resuming from checkpoint with ",
      length(processed_subject_sessions),
      " processed sessions\n", sep = "")
} else {
  bundle_stats_df <- tibble()
  processed_subject_sessions <- character(0)
}

subject_sessions <- sort(valid_sessions)
i <- 0

for (ss in subject_sessions) {

  if (ss %in% processed_subject_sessions) {
    next
  }

  cat("Processing ", ss, "\n", sep = "")

  new_entry <- process_subject(ss)

  if (!is.null(new_entry)) {
    bundle_stats_df <- bind_rows(bundle_stats_df, new_entry)
  }

  i <- i + 1

  if (i %% 5000 == 0) {
    write_parquet(bundle_stats_df, bundle_stats_checkpoint)
    cat("Checkpoint saved after ", i, " new subjects\n", sep = "")
  }
}

write_parquet(bundle_stats_df, final_bundle_stats_file)
cat("Saved combined bundle/scalar stats to: ", final_bundle_stats_file, "\n")

# ============================================================
# STEP 4: EXCLUSION no_qsirecon_exclude
# ============================================================
no_qsirecon_sessions <- setdiff(
  merged_df$subject_session,
  bundle_stats_df$subject_session
)

merged_df <- merged_df %>%
  mutate(no_qsirecon_exclude = subject_session %in% no_qsirecon_sessions)

log_exclusions(merged_df, "no_qsirecon_exclude")

# ============================================================
# STEP 5: FINAL MERGE
# ============================================================
log_info("STEP 5: Final merge")

merged_all <- merged_df %>%
  left_join(bundle_stats_df, by = "subject_session")

merged_all <- merged_all %>%
  select(
    -matches("^(bc_|pconn|n=)"),
    -any_of(c(
      "task_id", "dir_id", "acq_id", "space_id", "rec_id", "run_id",
      "#subcortical_segmentation_vol_out(n=22)",
      "#cortical_morphometry_sulc_out(n=333)",
      "5min_pconn_Gordon2014", "10min_pconn_Gordon2014",
      "#pconn_out_5min_Gordon2014(n=61776)", "#pconn_out_10min_Gordon2014(n=61776)",
      "5min_pconn_HCP2016", "10min_pconn_HCP2016",
      "#pconn_out_5min_HCP2016(n=61776)", "#pconn_out_10min_HCP2016(n=61776)",
      "5min_pconn_Markov2012", "10min_pconn_Markov2012",
      "#pconn_out_5min_Markov2012(n=61776)", "#pconn_out_10min_Markov2012(n=61776)",
      "5min_pconn_Power2011", "10min_pconn_Power2011",
      "#pconn_out_5min_Power2011(n=61776)", "#pconn_out_10min_Power2011(n=61776)",
      "5min_pconn_Yeo2011", "10min_pconn_Yeo2011",
      "#pconn_out_5min_Yeo2011(n=61776)", "#pconn_out_10min_Yeo2011(n=61776)"
    ))
  )

whole_brain <- read_parquet(whole_brain_measures_path)
merged_all <- merged_all %>% left_join(whole_brain, by = "subject_session")

merged_all <- merged_all %>%
  rename_with(~ str_replace(.x, "DKI_dki_", "DKI_")) %>%
  rename_with(~ str_replace(.x, "GQI_dti_", "GQI_"))

vols <- read_csv(vol_csv, show_col_types = FALSE)
names(vols)[1:2] <- c("subject_session", "estimated_brain_volume")
merged_all <- merged_all %>% left_join(vols, by = "subject_session")

# ============================================================
# STEP 6: FINAL DATA TYPES
# ============================================================
log_info("Enforcing data types")

# Age -> numeric
if ("age" %in% names(merged_all)) {
  merged_all <- merged_all %>%
    mutate(age = suppressWarnings(as.numeric(age)))
}

# QC numeric cols
qc_numeric_cols <- c(
  "raw_dimension_x","raw_dimension_y","raw_dimension_z",
  "raw_voxel_size_x","raw_voxel_size_y","raw_voxel_size_z",
  "raw_max_b","raw_neighbor_corr","raw_masked_neighbor_corr",
  "raw_dwi_contrast","raw_num_bad_slices","raw_num_directions",
  "raw_coherence_index","raw_incoherence_index",
  "t1_dimension_x","t1_dimension_y","t1_dimension_z",
  "t1_voxel_size_x","t1_voxel_size_y","t1_voxel_size_z",
  "t1_max_b","t1_neighbor_corr","t1_masked_neighbor_corr",
  "t1_dwi_contrast","t1_num_bad_slices","t1_num_directions",
  "t1_coherence_index","t1_incoherence_index",
  "t1post_dimension_x","t1post_dimension_y","t1post_dimension_z",
  "t1post_voxel_size_x","t1post_voxel_size_y","t1post_voxel_size_z",
  "t1post_max_b","t1post_neighbor_corr","t1post_masked_neighbor_corr",
  "t1post_dwi_contrast","t1post_num_bad_slices","t1post_num_directions",
  "t1post_coherence_index","t1post_incoherence_index",
  "mean_fd","max_fd","max_rotation","max_translation",
  "max_rel_rotation","max_rel_translation","t1_dice_distance",
  "CNR0_mean","CNR1_mean","CNR2_mean","CNR3_mean","CNR4_mean",
  "CNR0_median","CNR1_median","CNR2_median","CNR3_median","CNR4_median",
  "CNR0_standard_deviation","CNR1_standard_deviation",
  "CNR2_standard_deviation","CNR3_standard_deviation",
  "CNR4_standard_deviation"
)
qc_numeric_cols <- qc_numeric_cols[qc_numeric_cols %in% names(merged_all)]

merged_all <- merged_all %>%
  mutate(across(all_of(qc_numeric_cols), ~ suppressWarnings(as.numeric(.x))))

# Bundle stats numeric
bundle_stat_patterns <- c("DSI", "DKI", "MAPMRI", "NODDI", "GQI", "msmt_")
pattern <- paste(bundle_stat_patterns, collapse = "|")
bundle_stat_cols <- names(merged_all)[grepl(pattern, names(merged_all))]

merged_all <- merged_all %>%
  mutate(across(all_of(bundle_stat_cols), ~ suppressWarnings(as.numeric(.x))))

# ============================================================
# EXCLUSION FLAGS: unify, logical, NA->FALSE
# ============================================================
exclusion_cols <- c(
  "atk_exclude",
  "no_qsiprep_exclude",
  "site_888_exclude",
  "site_22_exclude",
  "bad_site_manufacturer_combo_exclude",
  "no_qsirecon_exclude"
)

merged_all <- merged_all %>%
  mutate(
    across(
      all_of(exclusion_cols),
      ~ if_else(.x %in% c(TRUE, "TRUE", 1, "1"), TRUE, FALSE)
    )
  )

# Final keep flag
merged_all <- merged_all %>%
  mutate(
    not_excluded =
      !atk_exclude &
      !no_qsiprep_exclude &
      !site_888_exclude &
      !site_22_exclude &
      !bad_site_manufacturer_combo_exclude &
      !no_qsirecon_exclude
  )

# Everything else -> character
protected_numeric <- c(qc_numeric_cols, bundle_stat_cols,
                       "age", "estimated_brain_volume")

merged_all <- merged_all %>%
  mutate(across(
    .cols = setdiff(names(merged_all),
                    c(exclusion_cols, "not_excluded", protected_numeric)),
    as.character
  ))

# Make subject_session first
merged_all <- merged_all %>%
  relocate(subject_session, .before = 1)

write_parquet(merged_all, final_output_file)
log_info("FINAL SAVED: ", final_output_file)