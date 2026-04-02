#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=36G
#SBATCH --time=24:00:00
#SBATCH --output=../logs/abcc-%A_%a.log
#SBATCH --array=1-38
#SBATCH --tmp=250G

# Check for slurm
[ -z "${JOB_ID}" ] && JOB_ID=TEST

if [[ ! -z "${SLURM_JOB_ID}" ]]; then
    echo SLURM detected
    JOB_ID="${SLURM_JOB_ID}"
    NSLOTS="${SLURM_JOB_CPUS_PER_NODE}"
fi

# If on MSI do some additional setup
if [ "$USER" == "ciesl012" ]; then
    TMP=/scratch.local/"job-${JOB_ID}_${SLURM_ARRAY_TASK_ID}"
    mkdir -p "${TMP}"
    export S3_ENDPOINT_URL='https://s3.msi.umn.edu'
    DATA=`s3info keys --machine-output`
    if [[ $? -eq 0 ]];
    then
          read -r ACCESS_KEY SECRET_KEY <<< "$DATA";
          export AWS_ACCESS_KEY=$ACCESS_KEY;
          export AWS_SECRET_KEY=$SECRET_KEY;
    fi
fi

SIMG="${HOME}"/s3qsiprep/images/qsiprep-0.21.4.sif
CODE_DIR="${HOME}"/s3qsiprep/code
BUCKET_LIST="${HOME}"/s3qsiprep/subject_lists/subject_list.txt

# fail whenever something is fishy, use -x to get verbose logfiles
set -e -u -x

# Set up the remotes and get the subject id from the call
bucket=$(head -n ${SLURM_ARRAY_TASK_ID} ${BUCKET_LIST} | tail -n 1)
subid=$(echo ${bucket} | cut -d '/' -f 5)
sesid=$(echo ${bucket} | cut -d '/' -f 6)

# Use $TMP as the workdir
WORKDIR=${TMP}/"job-${JOB_ID}_${subid}_${sesid}"
mkdir -p "${WORKDIR}"
cd ${WORKDIR}

# Download the data
mkdir -p BIDS/${subid}/${sesid}

# Sleep a bit to avoid ddosing msi
sleep $((RANDOM % 600))
s3cmd get \
    --recursive \
    --exclude='*func*' \
    ${bucket} \
    BIDS/${subid}/${sesid}/

# Copy the files we need from the source directory
cp $CODE_DIR/eddy_params.json ./
cp $CODE_DIR/license.txt ./
cp $CODE_DIR/dataset_description.json BIDS/

# Check to see if we need to filter out the non-normalized T1w/T2ws
BIDS_FILTER=""
if find BIDS -name '*T1w.*' | grep -iq rec-norm; then
    echo Found rec-normalized, using BIDS filter
    cp $CODE_DIR/bids_filter.json ./
    BIDS_FILTER="--bids-filter-file ${PWD}/bids_filter.json"
fi

# Do we need to change the PhaseEncodingDirection in the fmap?
if ! grep '"PhaseEncodingDirection": "j-"'  BIDS/*/*/fmap/*.json
then
    echo No j- found in fmap jsons: adding one to dir-AP
    ap_json="$(find ./BIDS -name '*acq-dwi_dir-AP_epi.json')"
    if [ -z "${ap_json}" ]; then
	echo "No dir-AP fmap json found"
	exit 1
    fi
    sed -i 's/"PhaseEncodingDirection": "j",/"PhaseEncodingDirection": "j-",/g' "${ap_json}"
fi

# Remove decimals from the bval files
sed -i 's/\.[0-9][0-9]*//g' $(find ./BIDS -name '*.bval')

# Add a bidsignore for some files we don't want
## the actual compute job specification
cat >> BIDS/.bidsignore << "EOT"
**/fmaps/*.bval
**/fmaps/*.bvec
**/fmaps/*_task-*
**/*.html
EOT

# Do the run
qsiprep_failed=0
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

# If qsiprep failed we need to know about it - upload the log to s3
if [ ${qsiprep_failed} -gt 0 ]; then
    echo QSIPREPFAIL
    logfile=${HOME}/s3qsiprep/logs/abcc-${JOB_ID}_${SLURM_ARRAY_TASK_ID}.log
    FAIL_BUCKET="s3://abccqsiprepstaging/failures/${subid}/${sesid}"
    fail_logs=$(find results -name 'crash*.txt')

    # Copy the failed subject log to the bucket
    upload_failed=0
    s3cmd put ${logfile} ${fail_logs} ${FAIL_BUCKET}/ || upload_failed=1

    rm -rf "${WORKDIR}"
    exit 1
fi

## rename html to include sesid
cd results
html=qsiprep/${subid}/${subid}_${sesid}.html
mv qsiprep/${subid}.html ${html}

## rename figures files in to include session name
mv qsiprep/${subid}/figures/${subid}_t1_2_mni.svg \
   qsiprep/${subid}/figures/${subid}_${sesid}_t1_2_mni.svg
mv qsiprep/${subid}/figures/${subid}_seg_brainmask.svg \
   qsiprep/${subid}/figures/${subid}_${sesid}_seg_brainmask.svg
mv qsiprep/${subid}/figures qsiprep/${subid}/${sesid}/figures

## rename anat files to include session name
for anatfile in $(find ./qsiprep/${subid}/anat -type f); do
    renamed=$(echo ${anatfile} | sed "s/${subid}_/${subid}_${sesid}_/")
    mv ${anatfile} ${renamed}
    mv ${renamed} ./qsiprep/${subid}/${sesid}/anat/
done

# remove empty outside anat dir
rm -rf qsiprep/${subid}/anat

# Make the necessary changes to the html file
sed -i "s:./${subid}/figures:\./${sesid}/figures:g" ${html}
sed -i "s/_t1_2_mni.svg/_${sesid}_t1_2_mni.svg/g" ${html}
sed -i "s/_seg_brainmask.svg/_${sesid}_seg_brainmask.svg/g" ${html}


# Upload the SESSION results to s3
RESULTS_BUCKET="s3://midb-abcd-main-pr/derivatives/qsiprep_v0.21.4/${subid}"
s3cmd put --recursive qsiprep/${subid}/* ${RESULTS_BUCKET}/

echo SUCCESS