# Preprocessing
```{warning}
The full script cannot be run again in its current form because the S3 bucket used for centralized data storage that we upload QSIPrep results to is no longer in service since the data release was completed. The code snippets are provided here as a reference for what options were used. We do not encourage rerunning this code or reprocessing the raw data, and instead we encourage you to download the preprocessed data directly from NBDC if needed.

```
For the current iteration of ABCC 3.1.0, we used [QSIPrep](https://qsiprep.readthedocs.io/en/latest/usage.html) {cite}`cieslak2021qsiprep` to preprocess the data.

QSIPrep 0.21.4 was run with the following script (CUBIC:`/cbica/projects/abcd_qsiprep/s3qsiprep/code/s3_qsiprep.sh`, [GitHub](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/0_processing/qsiprep/s3_qsiprep.sh)) with the options:

```bash
singularity run \
    --containall \
    -B ${PWD} \
    ${SIMG} \
    ${PWD}/BIDS \
    ${PWD}/results \
    participant \
    -w ${PWD}/wkdir \
    --stop-on-first-crash \
    --skip-bids-validation \
    --fs-license-file ${PWD}/license.txt \
    ${BIDS_FILTER} \
    --participant-label "$subid" \
    --unringing-method rpg \
    --output-resolution 1.7 \
    --eddy-config ${PWD}/eddy_params.json \
    --notrack -v -v \
    --nthreads ${NSLOTS} \
    --omp-nthreads ${NSLOTS} || qsiprep_failed=1
```

and the following `eddy_params.json` file (CUBIC: `/cbica/projects/abcd_qsiprep/s3qsiprep/code/eddy_params.json`, [GitHub](https://github.com/PennLINC/Meisler_ABCD_dMRI/tree/main/scripts/0_processing/qsiprep/eddy_params.json)):
```json
{
  "flm": "quadratic",
  "slm": "none",
  "fep": false,
  "interp": "spline",
  "nvoxhp": 1000,
  "fudge_factor": 10,
  "dont_sep_offs_move": false,
  "dont_peas": false,
  "niter": 5,
  "method": "jac",
  "repol": true,
  "num_threads": 1,
  "is_shelled": true,
  "use_cuda": false,
  "cnr_maps": true,
  "residuals": false,
  "output_type": "NIFTI_GZ",
  "args": "--fwhm=10,0,0,0,0 "
}
```

```{note}
Due to legal reasons, we are unable to share the ABCD subject ID list and the FreeSurfer license file used for this preprocessing.
```
Results were uploaded to UMinn's intermediate storage AWS S3 bucket, which no longer exists.