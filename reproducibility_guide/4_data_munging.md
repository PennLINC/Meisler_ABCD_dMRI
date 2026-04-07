# Data Munging, Cleaning, and Tract-Based Quality Control

```{warning}
Some file naming and organization conventions may have changed in subsequent QSIPrep/QSIRecon releases, so the scripts provided here may not work for your data as is.
```

For convenience, the next step is to merge all of the demographic, image QC, and white matter measures to a single parquet file. This is done by submitting:

```bash
export CONFIG_PATH="/absolute/path/to/your/config.json" # If not already defined
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
cd "${PROJECT_ROOT}/scripts/1_munge_data"
sbatch munge_data.sh
```

which in turn runs [scripts/1_munge_data/munge_data.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/1_munge_data/munge_data.R). This produces `${PROJECT_ROOT}/data/raw_data/merged_data.parquet`.

```{note}
When launched via `sbatch`, logs are written under `scripts/1_munge_data/logs`.
```


## The basic idea

The basic idea is to read in all of the files, combine them, and then write a single parquet file. Each row corresponds to a single subject-session, (e.g., `sub-XX_ses-YY`). This is an exceptionally **long** spreadsheet, as there are 67 bundles, each with several summary metrics (microstructural and macrostructural).

## Input files

Demographic information comes from the ABCD BIDS dataset. In particular, the overall `participants.tsv` is used to get session-invariant demographics, such as sex and race. The subject-wise `sub-XX/sub-XX_sessions.tsv` is used to get fields such as `age, site, scanner_manufacturer, scanner_software, scanner_model` for each session. In the current scripts, these paths are resolved from `lasso_root` in `config.json` (for example, `${LASSO_ROOT}/abcc/rawdata` and `${LASSO_ROOT}/dairc`).

Image QC information comes from *QSIPrep* outputs, in particular in the `sub-XX/ses-YY/dwi/sub-XX_ses-YY_desc-ImageQC_dwi.csv` files. These contain all the image quality metrics, such as CNR, NDC, dMRI Contrast, etc. For this purpose, we have combined all of them, row-wise, to a single spreadsheet called `${PROJECT_ROOT}/data/raw_data/abcc_0.21.4_scanner_qc.csv`. These will eventually be tabular data on LASSO as well.

All of the bundle-wise measures live in the *QSIRecon* outputs under `${LASSO_ROOT}/abcc/derivatives`. Macrostructural bundle metrics are in `qsirecon-MSMTAutoTrack/sub-XX/ses-YY/dwi/sub-XX_ses-YY_space-T1w_bundles-MSMTAutoTrack_scalarstats.tsv`, and microstructural bundle metrics are in `qsirecon-${WORKFLOW}/sub-XX/ses-YY/dwi/*bundlestats.csv`.

## Flagging sessions for exclusion

For completeness, we include all information in this spreadsheet, *including sessions we will not analyze*. However, we add columns denoting different exclusion criteria that should be adhered to.

`atk_exclude`: `TRUE` if sessions failed Matt's AutoTrack exclusion criteria.
`fasttrack_exclude`: `TRUE` if DAIRC FastTrack marks the session as not usable (`fasttrack_usable != TRUE`).
`no_qsiprep_exclude`: `TRUE` indexed by if the key QSIPrep QC field `t1_neighbor_corr` is missing.
`site_888_exclude`: `TRUE` if `site` contains `888`.
`scanner_manufacturer_888_exclude`: `TRUE` if `scanner_manufacturer` contains `888`.
`missing_device_serial_exclude`: `TRUE` if `DeviceSerialNumber` is missing or empty.
`site_22_exclude`: `TRUE` if `site == "22"`.
`bad_site_manufacturer_combo_exclude`: `TRUE` for known implausible combinations (`site == "6"` with Philips, or `site == "9"` with GE).
`incomplete_qsirecon_exclude`: `TRUE` if any required QSIRecon component is missing (bundle stats, GQI, DKI, MAPMRI, or wmNODDI scalarstats).
`not_excluded`: `TRUE` only if all exclusion flags above are `FALSE`.

```{note}
`munge_data.R` also computes component-level flags (`no_bundlestats_exclude`, `no_GQI_exclude`, `no_DKI_exclude`, `no_MAPMRI_exclude`, `no_NODDI_exclude`) that are rolled up into `incomplete_qsirecon_exclude`.
```

## Filtering to the analysis dataset

After creating `merged_data.parquet`, we run [scripts/1_munge_data/filter_data_meisler_analyses.R](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/1_munge_data/filter_data_meisler_analyses.R) to create the analysis-filtered dataset:
`${PROJECT_ROOT}/data/raw_data/merged_data_meisler_analyses.parquet`.

This script does the following:

1. Loads `${PROJECT_ROOT}/data/raw_data/merged_data.parquet` (from `munge_data.R`).
2. Keeps only sessions with `not_excluded == TRUE`.
3. Drops a set of ignored bundle families:
   - `bundle_ProjectionBrainstem_DentatorubrothalamicTract-lr*`
   - `bundle_ProjectionBrainstem_DentatorubrothalamicTract-rl*`
   - `bundle_Commissure_AnteriorCommissure*`
   - `bundle_ProjectionBrainstem_CorticobulbarTractL*`
   - `bundle_ProjectionBrainstem_CorticobulbarTractR*`
4. Requires complete data (no `NA`) across all required `bundle_` columns.
5. Creates `software_major` from `scanner_software` (GE/Siemens/Philips version bins).
6. Creates harmonization batch label `batch_device_software = DeviceSerialNumber + "." + software_major`.
7. Removes small batches, keeping only `batch_device_software` groups with at least 10 sessions.

You can run it with:
```bash
export CONFIG_PATH="/absolute/path/to/your/config.json" # If not already defined
PROJECT_ROOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["project_root"])' "$CONFIG_PATH")
R_ENV=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["r_env"])' "$CONFIG_PATH")
"${R_ENV}/bin/Rscript" "${PROJECT_ROOT}/scripts/1_munge_data/filter_data_meisler_analyses.R"
```

## Data Dictionary

```{raw} html
<div style="overflow-y: auto; max-height: 200px;">
<pre>
ABCC / QSIRecon Derived Dataset — Data Dictionary (Expanded)

1. Identifiers & Session Metadata
- subject_session: Unique identifier combining subject and session.
- subject_id: ABCD subject ID.
- session_id: Scan session (ses-00A, ses-02A).
- acq_time: Acquisition timestamp.
- age: Age at scan.
- sex: Sex assigned at birth.
- siblings_twins: Twin or sibling status.

2. Ethnicity Flags
Each is a binary indicator:
White, Black, Asian, American Indian, Alaska Native, Native Hawaiian, Samoan, Vietnamese, Other, Refuse to Answer.

3. Scanner Metadata & QC
- scanner_manufacturer, scanner_model, scanner_software: Scanner metadata.
- DeviceSerialNumber: Serial number.
- SoftwareVersions: Scanner software release.

4. Raw, Preprocessed T1, and Post-B1 QC Metrics
raw_*: QC on raw DWI.
t1_*: QC on preprocessed DWI before B1 bias correction.
t1post_*: QC after B1 bias correction.

Metric meanings:
- dimension_*: Image matrix size.
- voxel_size_*: Physical voxel size (mm).
- max_b: Maximum b-value.
- neighbor_corr: Neighboring slice correlation (higher is better).
- masked_neighbor_corr: Same but within brain mask.
- dwi_contrast: DWI contrast quality metric.
- num_bad_slices: Count of slices counted as artifactual by DSI Studio.
- num_directions: Diffusion direction count.
- coherence_index / incoherence_index: From Schilling et al., 2019.
- dice_distance: Dice dissimilarity between DWI and T1w
- mean_fd, max_fd: Framewise displacement motion metrics.
- rotation/translation metrics: Motion parameters.

5. Bundle-Level Diffusion Scalars
Model prefixes:
- DKI: Diffusion Kurtosis Imaging (DIPY).
- GQI: Generalized Q-Sampling Imaging (DSI Studio).
- MAPMRI: Mean Apparent Propagator MRI (TORTOISE).
- NODDI: Neurite Orientation Dispersion and Density Imaging (AMICO).

Columns are generated from QSIRecon `*_scalarstats.tsv` and appear as:
bundle_{BundleName}_{Model}_{Scalar}_{mean|median}

Examples:
- `bundle_Association_UncinateFasciculusL_DKI_fa_mean`
- `bundle_Commissure_CorpusCallosumBody_GQI_qa_median`
- `bundle_ProjectionBasalGanglia_OpticRadiationR_NODDI_icvf_mean`

Notes:
- `munge_data.R` strips duplicated scalar prefixes (for example `dki_`/`dti_`), so names are normalized as `DKI_ad`, `GQI_fa`, etc.
- Some scalar families are intentionally dropped before saving (`NODDI_directions`, `GQI_ha`, `GQI_txx`, `GQI_txy`, `GQI_txz`, `GQI_tyy`, `GQI_tyz`, `GQI_tzz`).

6. Bundle-Level Macrostructural Metrics
General pattern:
bundle_{BundleName}_{Metric}

Bundle names include the MSMT AutoTrack set, e.g.:
AssociationUncinateFasciculusL/R, AssociationExtremeCapsuleL/R, AssociationMiddleLongitudinalFasciculusL/R,
AssociationVerticalOccipitalFasciculusL/R, HippocampusAlveusL/R,
CommissureAnteriorCommissure, CorpusCallosum subdivisions,
ProjectionBasalGangliaOpticRadiationL/R, SuperiorLongitudinalFasciculus parts, etc.

Tract Macrostructural Metrics:
- 1st_quarter_volume_mm3
- 2nd_and_3rd_quarter_volume_mm3
- 4th_quarter_volume_mm3
- mean_length_mm
- span_mm
- number_of_tracts
- area_of_end_region_1_mm2 / area_of_end_region_2_mm2
- radius_of_end_region_1_mm / radius_of_end_region_2_mm
- total_area_of_end_regions_mm2
- total_surface_area_mm2
- total_volume_mm3
- total_radius_of_end_regions_mm
- volume_of_end_branches_1 / volume_of_end_branches_2
- irregularity, curl, elongation

7. Exclusion Flags
These describe why a session was removed:
- atk_exclude: Marked by AutoTrack QC as failed (geometry or very low tract count).
- fasttrack_exclude: FastTrack indicates the dMRI session is not usable.
- no_qsiprep_exclude: Missing QSIPrep QC metric (`t1_neighbor_corr`).
- site_888_exclude: Site code contains 888.
- scanner_manufacturer_888_exclude: Scanner manufacturer field contains 888.
- missing_device_serial_exclude: Device serial number is missing or empty.
- site_22_exclude: Site code is 22.
- bad_site_manufacturer_combo_exclude: Known incompatible combination (site 6 + Philips, or site 9 + GE).
- incomplete_qsirecon_exclude: At least one required QSIRecon component is missing.

8. Final Inclusion Status
- not_excluded: TRUE if all exclusion flags above are FALSE.
</pre>
</div>
```
