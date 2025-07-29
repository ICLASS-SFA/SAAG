#!/bin/bash
#SBATCH --job-name=saag2014
#SBATCH --account=m1657
#SBATCH --qos=regular
#SBATCH --time=6:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --constraint=cpu
#SBATCH --output=saag_subset_%j.out
#SBATCH --error=saag_subset_%j.err

# Optimize for AMD EPYC Milan CPUs
export OMP_NUM_THREADS=1
export MALLOC_ARENA_MAX=4
export TMPDIR=/tmp

# NUMA optimizations for AMD EPYC
export NUMEXPR_MAX_THREADS=128
export MKL_NUM_THREADS=1

# Set CPU affinity and memory policy for better NUMA performance
export GOMP_CPU_AFFINITY="0-127"
export OMP_PROC_BIND=spread
export OMP_PLACES=cores

# Load required modules (adjust for your system)
# module load python/3.9
# module load netcdf4-python

# Activate the Python environment
source activate /global/common/software/m1867/python/pyflex-dev

# Create temporary directory for this job with better I/O
export JOB_TMP_DIR="/tmp/saag_subset_${SLURM_JOB_ID}"
mkdir -p ${JOB_TMP_DIR}

# Use faster local storage if available (adjust path as needed)
# export JOB_TMP_DIR="/dev/shm/saag_subset_${SLURM_JOB_ID}"  # RAM disk if available
# mkdir -p ${JOB_TMP_DIR}

# Update config to use job-specific temp directory
CONFIG_FILE="config_subset_aux_goamazon_chunked.yml"
TEMP_CONFIG="config_subset_aux_goamazon_chunked_${SLURM_JOB_ID}.yml"

# Copy config and update temp directory
cp ${CONFIG_FILE} ${TEMP_CONFIG}
sed -i "s|dask_tmp_dir:.*|dask_tmp_dir: '${JOB_TMP_DIR}'|" ${TEMP_CONFIG}

echo "Starting SAAG subset processing at $(date)"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Using temporary directory: ${JOB_TMP_DIR}"
echo "Config file: ${TEMP_CONFIG}"

# Run the processing
python subset_vars_from_aux.py ${TEMP_CONFIG}

echo "SAAG subset processing completed at $(date)"

# Clean up temporary files
rm -rf ${JOB_TMP_DIR}
rm -f ${TEMP_CONFIG}

echo "Cleanup completed"
