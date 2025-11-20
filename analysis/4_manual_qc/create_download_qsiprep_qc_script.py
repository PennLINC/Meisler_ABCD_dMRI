import numpy as np
import os

# Load the rater_assignments data
file_path = "/cbica/projects/abcd_qsiprep/meisler_ge_philips/rater_assignments.npy"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"{file_path} not found!")

rater_assignments = np.load(file_path, allow_pickle=True).item()

# Output file for s5cmd commands
output_file = "/cbica/projects/abcd_qsiprep/meisler_ge_philips/download_qsiprep_qc.sh"

# S3 bucket and base path
#s3_base_path = "s3://midb-abcd-main-pr/derivatives/qsiprep_v0.21.4/"
s3_base_path = "s3://abccqsiprepstaging/results/"

# Prepare the list of subject-session combos
subject_sessions = set()
for rater, bins in rater_assignments.items():
    for bin_key, sub_ses_list in bins.items():
        subject_sessions.update(sub_ses_list)

# Generate s5cmd commands
commands = [
    f"s5cmd --numworkers 2 cp --flatten --no-clobber '{s3_base_path}{sub_ses.replace('_', '/')}/dwi/*dwiqc.json' /cbica/projects/abcd_qsiprep/meisler_ge_philips/qc_jsons/"
    for sub_ses in subject_sessions
]

# Write the commands to a shell script
with open(output_file, "w") as f:
    f.write("#!/bin/bash\n\n")
    f.write("\n".join(commands))

# Print confirmation
print(f"Shell script with download commands created: {output_file}")