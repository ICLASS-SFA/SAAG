#!/bin/bash
#SBATCH -A m1657
#SBATCH -J SAAGsubset
#SBATCH -t 06:00:00
##SBATCH -q regular
#SBATCH -C cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --qos=shared
#SBATCH --output=log_subset_saag_wrfout_%A_%a.log
#SBATCH --mail-type=END
#SBATCH --mail-user=zhe.feng@pnnl.gov
#SBATCH --array=1-23

date
# Activate E3SM environment
source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

# cd /global/homes/f/feng045/program/iclass/saag/

# Takes a specified line ($SLURM_ARRAY_TASK_ID) from the task file
LINE=$(sed -n "$SLURM_ARRAY_TASK_ID"p tasks_jobarray.txt)
echo $LINE
# Run the line as a command
$LINE

date
