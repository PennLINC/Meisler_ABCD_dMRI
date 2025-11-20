# Data Munging, Cleaning, and Tract-Based Quality Control
```{warning}
Some file naming conventions may have changed in subsequent QSIPrep/QSIRecon releases, so the scripts provided here may not work for your data as is.
```

For convenience, the next step is to merge all of the demographic, image QC, and white matter measures to a single parquet file. This is done by submitting:
```bash
sbatch /cbica/projects/abcd_qsiprep/meisler_abcd_paper/munge_data/munge_data.sh
```
which in turn runs (CUBIC: `/cbica/projects/abcd_qsiprep/meisler_abcd_paper/munge_data/munge_data_11172025.R`, [GitHub](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/3_data_munging/munge_data_11172025.R)). This produces the file `/cbica/projects/abcd_qsiprep/meisler_abcd_paper/munge_data/merged_data_11172025.parquet`.

### The basic idea
The basic idea is to read in all of the files, combine them, and then write a single parquet file. Each row corresponds to a single subject-session, (e.g., `sub-XX_ses-YY`). This is an exceptionally **long** spreadsheet, as there are 67 bundles, each with several summary metrics (microstructural and macrostructural).

### Input files
Demographic information comes from the ABCD BIDS dataset. In particular, the overall `participants.tsv` is used to get session invariant demographics, such as sex and race. The subject wise `sub-XX/sub-XX_sessions.tsv` is used to get fields such as `age, site, scanner_manufacturer, scanner_software, scanner_model` for each session. The demographics directory containing these files is set as `/cbica/projects/abcd_qsiprep/meisler_abcd_paper/munge_data/demographics`, but that can be adjusted. Note that this folder has all of the files copied / symlinkned into those driectories from the S3 buckets.

Image QC information comes from _QSIPrep_ outputs, in particular in the `sub-XX/ses-YY/dwi/sub-XX_ses-YY_desc-ImageQC_dwi.csv` files. These contain all the image quality metrics, such as CNR, NDC, dMRI Contrast, etc... For this purpose, we have combined all of them, row-wise, to a single spreadsheet called `/cbica/projects/abcd_qsiprep/meisler_abcd_paper/munge_data/demographics/abcc_0.21.4_scanner_qc.csv`.

All of the bundle-wise measures live in the _QSIRecon_ outputs. Macrostructural bundle metrics are in `qsirecon-MSMTAutoTrack/sub-XX/ses-YY/dwi/sub-XX_ses-YY_space-T1w_bundles-MSMTAutoTrack_scalarstats.tsv`, and microstructural bundle metrics are in `qsirecon-${WORKFLOW}$/sub-XX/ses-YY/dwi/*bundlestats.csv`.

### Flagging sessions for exclusion
For completeness, we include all information in this spreadsheet, _including for sessions we will not analayze_. However, we add columns denoting different exclusion criteria that should be adherred to.

`atk_exclude`: `TRUE` if sessions failed Matt's AutoTrack exclusion criteria (see below)
`no_qsiprep_exclude`: `TRUE` if sessions do not have _QSIPrep_ (either because it was never processed, failed, no DWI, or lost to data transfer issues)
`no_qsirecon_exclude`: `TRUE` if sessions do not have _QSIRecon_ (either because it was never processed, failed, no DWI, or lost to data transfer issues)
`site_888_exclude`: `TRUE` if the site is `888` (information not available)
`site_22_exclude`: `TRUE` if the site is `22` (not one of the main ABCD sites)
`bad_site_manufacturer_combo_exclude` `TRUE` for a three sessions who had Site 6 and Philips scanner, or Site 9 and GE scanner (both implausible)
`not_excluded`: `TRUE` if all exclusion flags above are `FALSE` - 25551 sessions!

```{note}
Note that we exclude a handful more subjects before running analyses due to some incomplete bundle reconstructions, as described later. The `not_excluded` flag is just a minimal recommended exclusion criteria.
```

### Data Dictionary

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

5. Whole-Brain and Tissue-Level Diffusion Scalars
Model prefixes:
- DKI: Diffusion Kurtosis Imaging.
- GQI: Generalized Q-Sampling Imaging.
- MAPMRI: Mean Apparent Propagator MRI.
- NODDI: Neurite Orientation Dispersion and Density Imaging.

Scalars:
- fa, md, rd, ad, mk, mkt, kfa
- qa, gfa, iso, ha, txx, txy, txz, tyy, tyz, tzz
- ng, ngpar, ngperp, pa, path, rtop, rtpp, rtap
- icvf, isovf, od, directions

Appear as:
WholeBrain_MODEL_scalar
AllGrayMatter_MODEL_scalar
AllWhiteMatter_MODEL_scalar

6. Bundle-Level Metrics (msmt_*)
General pattern:
msmt_{BundleName}_{Model?}_{Metric}

Bundle names include entire MSMT AutoTrack set, e.g.:
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
- no_qsiprep_exclude: Missing QSIPrep QC metric (t1_neighbor_corr).
- site_888_exclude: Sessions with invalid site AND missing DeviceSerialNumber.
- site_22_exclude: Site manually known to be invalid.
- bad_site_manufacturer_combo_exclude: Known incompatible combination (site 6 + Philips, site 6 + GE).
- no_qsirecon_exclude: Subject has no QSIRecon scalar or bundle statistics.

8. Final Inclusion Status
- not_excluded: TRUE if all exclusion flags above are FALSE.
</pre>
</div>
```