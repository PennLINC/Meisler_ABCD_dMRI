#!/bin/bash
#SBATCH --job-name=munge_data
#SBATCH --output=munge_data_%j.out
#SBATCH --error=munge_data_%j.err
#SBATCH --time=47:59:59
#SBATCH --mem=32G
#SBATCH --cpus-per-task=16

/cbica/projects/abcd_qsiprep/miniconda3/envs/combat_fam/bin/Rscript /cbica/projects/abcd_qsiprep/meisler_abcd_paper/munge_data/munge_data_11172025.R