suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(purrr)
})

# Paths
mean_dir   <- "/cbica/projects/abcd_qsiprep/sisk_myelin_dev/analysis"
demos_file <- "/cbica/projects/abcd_qsiprep/meisler_abcd_paper/munge_data/outputs_11172025/merged_demos_qc.parquet"
out_file   <- "/cbica/projects/abcd_qsiprep/sisk_myelin_dev/analysis/all_mean_values_merged.parquet"

# Load the bundle stats data (for subject_session list)
message("Loading demo data...")
demos <- read_parquet(demos_file)
subject_sessions <- demos$subject_session

# Helper: parse one mean_values.csv
parse_mean_file <- function(file) {
  message("Processing: ", file)

  df <- tryCatch(read.csv(file, stringsAsFactors = FALSE), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0) {
    warning("Empty or unreadable file: ", file)
    return(NULL)
  }

  subj <- df$subject[1]
  ses  <- df$session[1]
  subj_ses <- paste0(subj, "_", ses)

  # Split metric into metric and model
  df <- df %>%
    tidyr::separate(metric, into = c("metric", "model"), sep = "\\|", fill = "right")

  # Normalize model names
  df$model <- dplyr::case_when(
    df$model == "wmNODDI" ~ "NODDI",
    df$model == "gmNODDI" ~ "gmNODDI",
    stringr::str_detect(df$model, regex("GQI|gqi|rdi")) ~ "GQI",
    stringr::str_detect(df$model, regex("DKI|dki")) ~ "DKI",
    stringr::str_detect(df$model, regex("MAPMRI|mapmri")) ~ "MAPMRI",
    TRUE ~ df$model
  )

  # Construct final column names
  df <- df %>%
    mutate(
      AllGrayMatter_col = paste0("AllGrayMatter_", model, "_", metric),
      AllWhiteMatter_col = paste0("AllWhiteMatter_", model, "_", metric),
      WholeBrain_col     = paste0("WholeBrain_", model, "_", metric)
    )

  # Collect values as a named list
  vals <- c(
    setNames(df$gm_mean, df$AllGrayMatter_col),
    setNames(df$wm_mean, df$AllWhiteMatter_col),
    setNames(df$brain_mean, df$WholeBrain_col)
  )

  # Return a one-row data.frame
  out <- data.frame(subject_session = subj_ses, t(as.data.frame(vals)), check.names = FALSE)
  return(out)
}

# Find all mean_values.csv files
message("Searching for mean_values.csv files...")
files <- Sys.glob(file.path(mean_dir, "sub-*", "ses-*", "sub-*_ses-*_mean_values.csv"))
message("Found ", length(files), " files.")

# Parse all files and combine
parsed <- purrr::map_dfr(files, parse_mean_file)

# Identify missing subject_sessions
missing_sessions <- setdiff(subject_sessions, parsed$subject_session)
if (length(missing_sessions) > 0) {
  message("Missing mean_values.csv for ", length(missing_sessions), " subject_sessions:")
  message(paste(missing_sessions, collapse = "\n"))
}

# Merge with demos and sort
final_df <- demos %>%
  select(subject_session) %>%
  left_join(parsed, by = "subject_session") %>%
  arrange(subject_session)

# Save as Parquet
write_parquet(final_df, out_file)
message("Saved merged parquet to: ", out_file)