import os
import re
import gc
import sys
import numpy as np
import nibabel as nib
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from glob import glob
from nilearn.image import load_img, resample_to_img
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


# # CUBIC paths
path = '/cubic/projects/abcd_qsiprep/recon_results/derivatives'
templatedir = '/cubic/projects/abcd_qsiprep/sisk_myelin_dev/templateflow'
out_dir = '/cubic/projects/abcd_qsiprep/sisk_myelin_dev/analysis'

# Define spatial parameters and load tissue masks for brain segmentation analysis
dwi_shape = (95, 116, 101)
dwi_affine = np.array([[ -1.70000005,   0.        ,   0.        ,  79.8999939 ],
       [  0.        ,  -1.70000005,   0.        ,  80.24999237],
       [  0.        ,   0.        ,   1.70000005, -80.        ],
       [  0.        ,   0.        ,   0.        ,   1.        ]])

# Mask paths
gmmask_path = templatedir + '/tpl-MNI152NLin2009cAsym/tpl-MNI152NLin2009cAsym_res-02_label-GM_probseg.nii.gz'
wmmask_path = templatedir + '/tpl-MNI152NLin2009cAsym/tpl-MNI152NLin2009cAsym_res-02_label-WM_probseg.nii.gz'
brainmask_path = templatedir + '/tpl-MNI152NLin2009cAsym/tpl-MNI152NLin2009cAsym_res-02_desc-brain_mask.nii.gz'

# =============================================================================
# FUNCTIONS
# =============================================================================

def load_mask_data(mask_img, shape, affine, out_dir=None):
    """
    Load a NIfTI mask, resample to target shape/affine if needed, threshold >0.05 to boolean.
    Optionally save the resampled/thresholded mask when out_dir is provided.
    """
    if mask_img is None:
        return None

    m = load_img(mask_img)
    if m.shape != shape or not np.allclose(m.affine, affine):
        m = resample_to_img(
            m,
            nib.Nifti1Image(np.zeros(shape, dtype=np.float32), affine),
            interpolation="nearest"
        )

    mdata = m.get_fdata()
    mdata = (mdata >= 0.05).astype(bool)

    if out_dir is not None:
        out_dir = out_dir + "/masks"
        os.makedirs(out_dir, exist_ok=True)
        out_name = os.path.basename(mask_img).replace(".nii.gz", "_parcellated.nii.gz")
        nib.save(nib.Nifti1Image(mdata.astype(np.uint8), affine), out_dir + "/" + out_name)

    return mdata


def compute_means(sub, ses, metric, model, fpath, gmmaskdata, wmmaskdata, brainmaskdata):
    """
    Compute mean values for GM, WM, and brain regions from a NIfTI file.

    Args:
        sub (str): Subject identifier
        ses (str): Session identifier
        metric (str): Metric name
        model (str): Model name
        fpath (str): Path to NIfTI file
        gmmaskdata (np.ndarray): Gray matter mask
        wmmaskdata (np.ndarray): White matter mask
        brainmaskdata (np.ndarray): Brain mask

    Returns:
        dict: Dictionary with subject, session, metric, model, and mean values
    """
    try:
        img = nib.load(fpath)
        # Use float64 to avoid overflow, then convert to float32 for memory efficiency
        data = img.get_fdata(dtype=np.float64)

        # Check for extreme values that might cause overflow
        if np.any(np.isnan(data)) or np.any(np.isinf(data)):
            print(f"  WARNING: {sub} {ses} {metric}|{model} contains NaN or Inf values")
            return {"subject": sub, "session": ses, "metric": metric, "model": model, "gm": np.nan, "wm": np.nan, "brain": np.nan}

        # Convert to float32 for memory efficiency, but handle potential overflow
        try:
            data = data.astype(np.float32)
        except OverflowError:
            print(f"  WARNING: {sub} {ses} {metric}|{model} data overflow - clipping extreme values")
            data = np.clip(data, -1e6, 1e6).astype(np.float32)

        # Compute means using cleaned data (plotting commented out for cluster use)
        # print(f"Processing data for {sub} {ses} {metric}|{model}...")

        # # Create output directory for plots (commented out for cluster)
        # plot_dir = os.path.join(out_dir, 'brain_plots')
        # os.makedirs(plot_dir, exist_ok=True)

        # Define tissue types and their corresponding masks
        tissue_types = [
            ('GM', gmmaskdata, 'Gray Matter'),
            ('WM', wmmaskdata, 'White Matter'),
            ('Brain', brainmaskdata, 'Brain')
        ]

        # Process each tissue type: create cleaned data and compute mean
        tissue_means = {}

        for tissue_code, mask_data, tissue_name in tissue_types:
            if mask_data is not None:
                # Create cleaned masked data (single operation)
                masked_data = data.copy()
                masked_data[~mask_data] = 0  # Set non-tissue voxels to 0

                # Debug: Check for non-zero values in background (commented out for cluster)
                # background_voxels = masked_data[~mask_data]
                # non_zero_bg = np.count_nonzero(background_voxels)
                # if non_zero_bg > 0:
                #     print(f"  WARNING: {non_zero_bg} non-zero values in background for {tissue_name}")
                #     print(f"    Min background value: {np.min(background_voxels)}")
                #     print(f"    Max background value: {np.max(background_voxels)}")

                # Debug: Check data range (commented out for cluster)
                # print(f"  {tissue_name} data range: {np.min(masked_data):.6f} to {np.max(masked_data):.6f}")
                # print(f"  {tissue_name} zero count: {np.count_nonzero(masked_data == 0)} / {masked_data.size}")

                # # Create figure for this tissue type with 4 views (commented out for cluster)
                # fig, axes = plt.subplots(2, 2, figsize=(12, 10))
                # fig.suptitle(f'{sub} {ses} {metric}|{model} - {tissue_name}\nFile: {os.path.basename(fpath)}', fontsize=12)
                #
                # # Get dimensions for slice selection
                # h, w, d = data.shape
                #
                # # 1. Original data (axial view - middle slice)
                # axes[0, 0].imshow(data[:, :, d//2], cmap='hot', aspect='auto', vmin=0)
                # axes[0, 0].set_title('Original Data (Axial)')
                # axes[0, 0].axis('off')
                #
                # # 2. Sagittal view (just off midline)
                # sagittal_slice = w//2 + 5  # Slightly off midline
                # axes[0, 1].imshow(np.rot90(masked_data[sagittal_slice, :, :]), cmap='hot', aspect='auto', vmin=0)
                # axes[0, 1].set_title(f'{tissue_name} Masked Data (Sagittal)')
                # axes[0, 1].axis('off')
                #
                # # 3. Coronal view (midline)
                # coronal_slice = h//2
                # axes[1, 0].imshow(np.rot90(masked_data[:, coronal_slice, :]), cmap='hot', aspect='auto', vmin=0)
                # axes[1, 0].set_title(f'{tissue_name} Masked Data (Coronal)')
                # axes[1, 0].axis('off')
                #
                # # 4. Axial view (midline)
                # axial_slice = d//2
                # axes[1, 1].imshow(masked_data[:, :, axial_slice], cmap='hot', aspect='auto', vmin=0)
                # axes[1, 1].set_title(f'{tissue_name} Masked Data (Axial)')
                # axes[1, 1].axis('off')
                #
                # # Save plot for this tissue type
                # plot_file = os.path.join(plot_dir, f"{sub}_{ses}_{metric}_{model}_{tissue_code}_plot.png")
                # plt.tight_layout()
                # plt.savefig(plot_file, dpi=150, bbox_inches='tight')
                # plt.close()  # Close figure to free memory
                #
                # print(f"  Saved {tissue_name} plot: {plot_file}")

                # Compute mean directly from the cleaned data
                tissue_voxels = masked_data[mask_data]
                if tissue_voxels.size == 0:
                    tissue_means[tissue_code] = np.nan
                else:
                    tissue_means[tissue_code] = float(tissue_voxels.mean())
            else:
                tissue_means[tissue_code] = np.nan

        # Extract means (using fallback for missing tissues)
        gm = tissue_means.get('GM', np.nan)
        wm = tissue_means.get('WM', np.nan)
        brain = tissue_means.get('Brain', np.nan)

        # Debug: Print detailed information (commented out for cluster)
        # print(f"DEBUG: {sub} {ses} {metric}|{model}")
        # print(f"  GM: {gm} (type: {type(gm)})")
        # print(f"  WM: {wm} (type: {type(wm)})")
        # print(f"  Brain: {brain} (type: {type(brain)})")
        # print(f"  File: {os.path.basename(fpath)}")
        # print("---")

        return {"subject": sub, "session": ses, "metric": metric, "model": model, "gm": gm, "wm": wm, "brain": brain}
    except Exception as e:
        # Robust to corrupt/missing file reads
        return {"subject": sub, "session": ses, "metric": metric, "model": model, "gm": np.nan, "wm": np.nan, "brain": np.nan}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":

    # Get mask data
    gmmaskdata = load_mask_data(gmmask_path, dwi_shape, dwi_affine, out_dir)
    wmmaskdata = load_mask_data(wmmask_path, dwi_shape, dwi_affine, out_dir)
    brainmaskdata = load_mask_data(brainmask_path, dwi_shape, dwi_affine, out_dir)

    # Discover and parse brain imaging files across all subjects, sessions, and metrics
    # Create sets for later organization using the same regex pattern
    metrics_v1 = pd.Series(glob(path + '/qsirecon-*/sub-*/ses-*/dwi/sub-*_ses-00A_space-MNI152NLin2009cAsym_*param*.nii.gz'))

    # Use the same regex pattern to extract metrics consistently
    metric_re_for_set = re.compile(r'param-([A-Za-z0-9._-]+)_')
    model_re_for_set = re.compile(r'model-([A-Za-z0-9._-]+)_')
    metrics_list = []
    for file_path in metrics_v1:
        fname = os.path.basename(file_path)
        m_metric = metric_re_for_set.search(fname)
        m_model = model_re_for_set.search(fname)

        if m_metric:
            metric = m_metric.group(1)
            model = m_model.group(1) if m_model else None

            # Check if this is a NODDI reconstruction and determine type
            if 'noddi' in file_path.lower():
                if 'gmnoddi' in file_path.lower():
                    model = 'gmNODDI'
                elif 'wmnoddi' in file_path.lower():
                    model = 'wmNODDI'

            # Check for tensor-based reconstructions and determine type
            elif 'tensor' in file_path.lower() and model and 'tensor' in model.lower():
                # Extract reconstruction type from path
                if 'dki' in file_path.lower():
                    model = model.replace('tensor', 'DKItensor')
                elif 'gqi' in file_path.lower():
                    model = model.replace('tensor', 'GQItensor')
                elif 'mapmri' in file_path.lower():
                    model = model.replace('tensor', 'MAPMRItensor')

            # Create metric key with model info
            metric_key = f"{metric}|{model}" if model else metric
            metrics_list.append(metric_key)

    metrics_set = set(metrics_list)
    print(f"Found {len(metrics_set)} unique metrics:")
    for metric in sorted(metrics_set):
        print(f"  {metric}")

    session_set = set(['ses-00A', 'ses-02A', 'ses-04A', 'ses-06A'])

    # Single pass file discovery (fast) across all recons/subjects/sessions
    all_files = glob(path + '/qsirecon-*/sub-*/ses-*/dwi/*space-MNI152NLin2009cAsym*param-*_dwimap.nii.gz')

    sub_re = re.compile(r'/sub-([^/]+)/')
    ses_re = re.compile(r'/(ses-[^/]+)/')
    model_re = re.compile(r'model-([A-Za-z0-9._-]+)_')
    metric_re = re.compile(r'param-([A-Za-z0-9._-]+)_')

    records = []
    for f in all_files:
        fname = os.path.basename(f)
        m_metric = metric_re.search(fname)
        m_model = model_re.search(fname)
        if not m_metric:
            continue
        metric = m_metric.group(1)

        model = m_model.group(1) if m_model else None

        # Check if this is a NODDI reconstruction and determine type
        if 'noddi' in f.lower():
            if 'gmnoddi' in f.lower():
                model = 'gmNODDI'
                print(f"NODDI type detection: model -> {model} (gmNODDI)")
            elif 'wmnoddi' in f.lower():
                model = 'wmNODDI'
                print(f"NODDI type detection: model -> {model} (wmNODDI)")
            else:
                print(f"WARNING: NODDI file found but type unclear: {f}")

        # Check for tensor-based reconstructions and determine type
        elif 'tensor' in f.lower() and model and 'tensor' in model.lower():
            # Extract reconstruction type from path
            if 'dki' in f.lower():
                model = model.replace('tensor', 'DKItensor')
                print(f"Tensor type detection: model -> {model} (DKI)")
            elif 'gqi' in f.lower():
                model = model.replace('tensor', 'GQItensor')
                print(f"Tensor type detection: model -> {model} (GQI)")
            elif 'mapmri' in f.lower():
                model = model.replace('tensor', 'MAPMRItensor')
                print(f"Tensor type detection: model -> {model} (MAPMRI)")
            else:
                print(f"Tensor reconstruction found but type unclear: {f}")

        # Create metric key with model info for comparison
        metric_key = f"{metric}|{model}" if model else metric
        if metric_key not in metrics_set:
            print(f"SKIPPING: {metric_key} not in metrics_set")
            continue
        else:
            print(f"ACCEPTED: {metric_key}")
        m_sub = sub_re.search(f)
        m_ses = ses_re.search(f)
        if not (m_sub and m_ses):
            continue
        sub = 'sub-' + m_sub.group(1)
        ses = m_ses.group(1)
        if ses not in session_set:
            continue
        records.append((sub, ses, metric, model, f))

    print(f"Found {len(records)} valid files to process")

    # Create subject/session folder structure and process files
    print("Creating subject/session folder structure...")

    # Get unique subjects and sessions
    subjects = sorted(set([record[0] for record in records]))
    sessions = sorted(set([record[1] for record in records]))

    print(f"Found {len(subjects)} unique subjects: {subjects[:5]}...")
    print(f"Found {len(sessions)} unique sessions: {sessions}")

    # Create base output directory
    os.makedirs(out_dir, exist_ok=True)

    # Create subject folders and session subfolders
    for subject in subjects:
        subject_dir = os.path.join(out_dir, subject)
        os.makedirs(subject_dir, exist_ok=True)

        for session in sessions:
            session_dir = os.path.join(subject_dir, session)
            os.makedirs(session_dir, exist_ok=True)

    print(f"Created folder structure for {len(subjects)} subjects × {len(sessions)} sessions")

    # Process files and save mean values
    print("Processing files and saving mean values...")

    # Group records by subject and session for organized processing
    subject_session_data = {}

    for sub, ses, metric, model, fpath in records:
        key = (sub, ses)
        if key not in subject_session_data:
            subject_session_data[key] = []
        subject_session_data[key].append((metric, model, fpath))

    # Check for duplicate files
    print("Checking for duplicate files...")
    for (sub, ses), files in subject_session_data.items():
        file_paths = [f[2] for f in files]  # Extract file paths
        if len(file_paths) != len(set(file_paths)):
            print(f"ERROR: Duplicate files found for {sub}/{ses}")
            duplicates = [f for f in file_paths if file_paths.count(f) > 1]
            print(f"  Duplicate files: {set(duplicates)}")
            print("Script will now exit due to duplicate files.")
            sys.exit(1)

    print(f"Processing {len(subject_session_data)} subject-session combinations...")

    # Function to process a single subject-session combination
    def process_subject_session(sub_ses_data):
        (sub, ses), files = sub_ses_data
        print(f"Processing {sub}/{ses} ({len(files)} files)...")

        # Create results dictionary for this subject-session
        session_results = {
            'subject': sub,
            'session': ses,
            'metrics': {}
        }

        # Process each file for this subject-session
        for metric, model, fpath in files:
            try:
                # Compute means using the existing function
                result = compute_means(sub, ses, metric, model, fpath, gmmaskdata, wmmaskdata, brainmaskdata)

                # Store results by metric and model
                metric_key = f"{metric}|{model}" if model else metric

                # Check if this metric already exists (avoid overwriting)
                if metric_key in session_results['metrics']:
                    print(f"  WARNING: Metric {metric_key} already exists! Overwriting...")
                    print(f"    Old values: GM={session_results['metrics'][metric_key]['gm']}, WM={session_results['metrics'][metric_key]['wm']}, Brain={session_results['metrics'][metric_key]['brain']}")
                    print(f"    New values: GM={result['gm']}, WM={result['wm']}, Brain={result['brain']}")

                session_results['metrics'][metric_key] = {
                    'gm': result['gm'],
                    'wm': result['wm'],
                    'brain': result['brain']
                }

            except Exception as e:
                print(f"  Error processing {fpath}: {e}")
                continue

        # Save results for this subject-session
        if session_results['metrics']:
            # Create output file path
            output_file = os.path.join(out_dir, sub, ses, f"{sub}_{ses}_mean_values.json")

            # Save as JSON for easy reading
            import json
            with open(output_file, 'w') as f:
                json.dump(session_results, f, indent=2)

            # Also save as CSV for easy analysis
            csv_data = []
            for metric_key, values in session_results['metrics'].items():
                csv_data.append({
                    'subject': sub,
                    'session': ses,
                    'metric': metric_key,
                    'gm_mean': values['gm'],
                    'wm_mean': values['wm'],
                    'brain_mean': values['brain']
                })

            csv_df = pd.DataFrame(csv_data)
            csv_file = os.path.join(out_dir, sub, ses, f"{sub}_{ses}_mean_values.csv")
            csv_df.to_csv(csv_file, index=False)

            # Debug: Print CSV values for comparison (commented out for cluster)
            # print(f"CSV DEBUG for {sub}/{ses}:")
            # for metric_key, values in session_results['metrics'].items():
            #     print(f"  {metric_key}: GM={values['gm']}, WM={values['wm']}, Brain={values['brain']}")
            # print("---")

            print(f"  Saved {len(session_results['metrics'])} metrics to {output_file}")
            return True
        else:
            print(f"  No valid metrics found for {sub}/{ses}")
            return False

    # Parallel processing using ThreadPoolExecutor with reduced memory load
    max_workers = min(8, len(subject_session_data))  # Reduced from 32 to 8 cores
    print(f"Using {max_workers} parallel workers (reduced for memory management)...")

    # Process in smaller batches to reduce memory pressure
    batch_size = 50  # Process 50 subject-sessions at a time
    all_sub_ses_items = list(subject_session_data.items())
    total_batches = (len(all_sub_ses_items) + batch_size - 1) // batch_size

    processed_count = 0
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(all_sub_ses_items))
        batch_items = all_sub_ses_items[start_idx:end_idx]

        print(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_items)} items)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for this batch only
            future_to_sub_ses = {
                executor.submit(process_subject_session, sub_ses_data): sub_ses_data
                for sub_ses_data in batch_items
            }

            # Process completed tasks with progress bar for this batch
            for future in tqdm(as_completed(future_to_sub_ses), total=len(future_to_sub_ses), desc=f"Batch {batch_num + 1}"):
                try:
                    success = future.result()
                    if success:
                        processed_count += 1
                except Exception as e:
                    sub_ses_data = future_to_sub_ses[future]
                    print(f"Error processing {sub_ses_data[0]}: {e}")

        # Force garbage collection after each batch to free memory
        gc.collect()
        print(f"Batch {batch_num + 1} complete. Processed {processed_count} total so far.")

    print(f"Processing complete! Saved results for {processed_count} subject-session combinations")
    print(f"Results saved in: {out_dir}")
    print("Folder structure:")
    print(f"  {out_dir}/")
    print(f"    ├── sub-XXXX/")
    print(f"    │   ├── ses-00A/")
    print(f"    │   │   ├── sub-XXXX_ses-00A_mean_values.json")
    print(f"    │   │   └── sub-XXXX_ses-00A_mean_values.csv")
    print(f"    │   ├── ses-02A/")
    print(f"    │   └── ...")
    print(f"    └── ...")

    print("Script completed successfully!")