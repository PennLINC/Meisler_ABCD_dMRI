#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=18G
#SBATCH --time=9:00:00
#SBATCH --output=../logs/abcc_recon-%A_%a.log
#SBATCH --array=51-10000

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


RECON_OUTPUT_DIR="${HOME}"/recon_results
SIMG="${HOME}"/s3qsiprep/images/qsirecon-1.0.0.sif
CODE_DIR="${HOME}"/s3qsiprep/code
RESULTS_CSV="${HOME}"/s3qsiprep/results/abcc_0.21.4_release_qc.csv
TEMPLATEFLOW_HOME="${HOME}"/s3qsiprep/templateflow_home

mkdir -p "${TEMPLATEFLOW_HOME}"

[ -z "${JOB_ID}" ] && JOB_ID=TEST

if [[ ! -z "${SLURM_JOB_ID}" ]]; then
    echo SLURM detected
    JOB_ID="${SLURM_JOB_ID}"
    NSLOTS="${SLURM_JOB_CPUS_PER_NODE}"
fi

# fail whenever something is fishy, use -x to get verbose logfiles
set -e -u -x

# Set up the remotes and get the subject id from the call
subject_row=$(head -n $((${SLURM_ARRAY_TASK_ID} + 1)) ${RESULTS_CSV} | tail -n 1)
subid=$(echo $subject_row | sed 's/^.*\(sub-[A-Za-z0-9]*\).*$/\1/')
sesid=$(echo $subject_row | sed 's/^.*_\(ses-[A-Za-z0-9]*\).*$/\1/')

# Use $TMP as the workdir
WORKDIR=${TMP}/"job-${JOB_ID}_${subid}_${sesid}"
mkdir -p "${WORKDIR}"
cd ${WORKDIR}

# Download the data
mkdir -p qsiprep/${subid}/${sesid}

# Sleep a bit to avoid ddosing msi
sleep $((RANDOM % 6000))
bucket="s3://midb-abcd-main-pr-release/bids/derivatives/qsiprep_v0.21.4/${subid}/${sesid}"
s5cmd sync \
    --exclude='*figures*' \
    ${bucket}/* \
    qsiprep/${subid}/${sesid}/

# Copy the files we need from the source directory
cp $CODE_DIR/license.txt ./
cp $CODE_DIR/dataset_description.json qsiprep/
cp $CODE_DIR/ABCD_Recon.yml ./

# Do the run
qsiprep_failed=0
singularity run \
    --containall \
    -B ${PWD} \
    -B "${TEMPLATEFLOW_HOME}:/templateflow_home" \
    --env "TEMPLATEFLOW_HOME=/templateflow_home" \
    ${SIMG} \
    ${PWD}/qsiprep \
    ${PWD}/results \
    participant \
    -w ${PWD}/wkdir \
    --report-output-level session \
    --stop-on-first-crash \
    --fs-license-file ${PWD}/license.txt \
    --participant-label "$subid" \
    --recon-spec ${PWD}/ABCD_Recon.yml \
    --notrack -v -v \
    --nthreads ${NSLOTS} \
    --omp-nthreads ${NSLOTS} || qsiprep_failed=1

# If qsiprep failed we need to know about it - upload the log to s3
if [ ${qsiprep_failed} -gt 0 ]; then
    echo QSIPREPFAIL
    FAIL_BUCKET="s3://abccqsiprepstaging/failures_recon/${subid}/${sesid}"
    fail_logs=$(find results -name 'crash*.txt')

    # Copy the failed subject log to the bucket
    s3cmd put ${fail_logs} ${FAIL_BUCKET}/

    rm -rf "${WORKDIR}"
    exit 1
fi


final_dir=${RECON_OUTPUT_DIR}/${subid}
mkdir -p ${final_dir}
cp -rv \
    results/${subid}/* \
    ${final_dir}


RECON_DIRS="qsirecon-wmNODDI qsirecon-gmNODDI qsirecon-DIPYDKI qsirecon-DSIStudioGQI qsirecon-TORTOISE_model-MAPMRI qsirecon-MSMTAutoTrack"
for recon_dir in $RECON_DIRS
do
    final_dir=${RECON_OUTPUT_DIR}/derivatives/${recon_dir}/${subid}/${sesid}
    mkdir -p ${final_dir}
    cp -rv \
        results/derivatives/${recon_dir}/${subid}/${sesid}/* \
	${final_dir}
done

# Upload the SESSION results to s3
RESULTS_BUCKET="s3://abccqsiprepstaging/recon_benchmark/"
s3cmd put --recursive results/* ${RESULTS_BUCKET}

echo SUCCESS