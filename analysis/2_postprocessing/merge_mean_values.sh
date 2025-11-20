#!/bin/bash
#SBATCH --job-name=merge_mean_values
#SBATCH --output=/cbica/projects/abcd_qsiprep/sisk_myelin_dev/analysis/logs/merge_mean_values_%j.out
#SBATCH --error=/cbica/projects/abcd_qsiprep/sisk_myelin_dev/analysis/logs/merge_mean_values_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=24G
#SBATCH --cpus-per-task=8

# Go to a working directory (optional)
cd /cbica/projects/abcd_qsiprep/sisk_myelin_dev/analysis

# Run the R script
~/miniconda3/envs/combat_fam/bin/Rscript /cbica/projects/abcd_qsiprep/sisk_myelin_dev/analysis/merge_mean_values.R