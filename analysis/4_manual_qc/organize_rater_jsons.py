import numpy as np
import os
import shutil

# Load the rater_to_sub dictionary from the .npy file
rater_assignments = np.load("/cbica/projects/abcd_qsiprep/meisler_ge_philips/rater_assignments.npy", allow_pickle=True).item()

# Define the source directory where QC JSON files are located
source_dir = "/cbica/projects/abcd_qsiprep/meisler_ge_philips/qc_jsons/"

# Define the target directory where rater-specific folders will be created
target_dir = "/cbica/projects/abcd_qsiprep/meisler_ge_philips/qc_jsons/"

root_dwiqc_json_path = "/cbica/projects/abcd_qsiprep/meisler_ge_philips/dwiqc.json"

# Create rater-specific directories and copy the QC JSON files into appropriate folders
for rater, manufacturer_bins in rater_assignments.items():
    # Create the rater's folder if it doesn't exist
    rater_folder = os.path.join(target_dir, f'rater_{rater}')
    os.makedirs(rater_folder, exist_ok=True)

    # Loop through the manufacturer-session keys (e.g., ('GE', 1)) and subjects
    for (manufacturer, bin_id), subjects in manufacturer_bins.items():
        print(f"Processing manufacturer: {manufacturer}, bin ID: {bin_id}, subjects: {subjects}")
        # Create a folder for the manufacturer (no bin ID) in the rater's folder
        manufacturer_folder = os.path.join(rater_folder, manufacturer)
        os.makedirs(manufacturer_folder, exist_ok=True)

        # Copy the global "dwiqc.json" to the manufacturer folder
        if os.path.exists(root_dwiqc_json_path):
            target_dwiqc_path = os.path.join(manufacturer_folder, "dwiqc.json")
            shutil.copy(root_dwiqc_json_path, target_dwiqc_path)
            print(f"Copied {root_dwiqc_json_path} to {manufacturer_folder} as {target_dwiqc_path}")

        # Loop through the subjects to create their specific folder structure and copy files
        for subject in subjects:
            # Define the folder structure as GE/sub-X/ses-Y/dwi/
            subject_folder = os.path.join(manufacturer_folder, f"{subject.split('_')[0]}", f"{subject.split('_')[1]}", "dwi")
            os.makedirs(subject_folder, exist_ok=True)

            # The QC JSON filename matches the subject ID and session ID format
            qc_json_file = f"{subject}_dwiqc.json"
            source_file_path = os.path.join(source_dir, qc_json_file)

            if os.path.exists(source_file_path):
                target_file_path = os.path.join(subject_folder, qc_json_file)
                shutil.copy(source_file_path, target_file_path)
                print(f"Copied {qc_json_file} to {subject_folder}")
            else:
                print(f"File {qc_json_file} not found in {source_dir}")

print("All files have been organized!")