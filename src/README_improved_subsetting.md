# SAAG Data Subsetting - Improved Parallelization

This directory contains improved scripts for subsetting SAAG auxiliary data with better memory management and chunking strategies to handle large datasets (365 daily files → 364 file pairs → ~35,000 output files) without crashing.

## Key Improvements

1. **Chunked Processing**: Breaks the 364 daily file pairs into manageable chunks (default: 14 pairs = 2 weeks)
2. **Batch Processing**: Processes chunks in batches to avoid overwhelming the Dask scheduler
3. **Memory Management**: Configures Dask with memory limits and spill settings optimized for AMD EPYC hardware
4. **Progress Monitoring**: Provides tools to monitor progress and identify issues
5. **Resume Capability**: Can resume processing from where it left off with timestep-level precision
6. **Multiprocessing Fix**: Uses threads instead of processes to avoid Python multiprocessing bootstrap errors

## Data Structure Understanding

- **Input**: 365 daily files (one per day, ~6GB each, 96 timesteps per file)
- **Processing**: 364 file pairs (adjacent days: [day1, day2], [day2, day3], etc.)
- **Output**: ~35,000 files (96 timesteps × 365 days, excluding first timestep due to no precipitation rate)
- **Chunking**: Groups daily file pairs (not timesteps) for processing

## Files

### Core Processing
- `subset_vars_from_aux.py` - Main processing script (improved with chunking and AMD EPYC optimization)
- `config_subset_aux_goamazon_chunked.yml` - Configuration with optimized chunking parameters

### Monitoring and Resume
- `find_missing_timesteps.py` - Find missing individual timestep files (fixed path construction bug)
- `monitor_progress.py` - Monitor processing progress

### Testing and Debug
- `debug_file_pair.py` - Test single file pair processing (for debugging)

### SLURM Support
- `run_subset_slurm.sh` - SLURM job script template

## Configuration Parameters

Optimized parameters for AMD EPYC 7763 (128 cores, 512GB RAM, 4 NUMA domains):

```yaml
# Parallel processing setup
run_parallel: 1
nprocesses: 128  # Total CPU cores
dask_tmp_dir: '/tmp'

# Chunking parameters optimized for AMD EPYC 7763
chunk_size: 14  # Number of daily file pairs per chunk (14 = 2 weeks)
max_concurrent_chunks: 8  # Maximum chunks processed simultaneously

# Date range for processing
startdate: '20150101.0000'  # Change year as needed
enddate: '20151231.2359'    # Change year as needed
```

### Hardware-Specific Recommendations

**AMD EPYC 7763 (128 cores, 512GB RAM)**:
- `chunk_size: 14` (2 weeks of daily files)
- `max_concurrent_chunks: 8` (8 concurrent chunks)
- `n_workers: 64` with `threads_per_worker: 2`
- `memory_limit: 8GB` per worker

**Smaller systems (64 cores, 256GB RAM)**:
- `chunk_size: 7` (1 week of daily files)
- `max_concurrent_chunks: 4` (4 concurrent chunks)
- `n_workers: 32` with `threads_per_worker: 2`
- `memory_limit: 6GB` per worker

Each daily file pair produces ~96 output files, so:
- `chunk_size: 14` → ~1,344 output files per chunk
- `max_concurrent_chunks: 8` → ~10,752 output files being processed simultaneously

## Usage

### 1. Configure for your year and hardware

Edit `config_subset_aux_goamazon_chunked.yml`:
```yaml
# Change dates for your target year
startdate: '20160101.0000'  # Example: 2016
enddate: '20161231.2359'

# Adjust paths as needed
root_dir: '/pscratch/sd/f/feng045/SAAG/hist/auxhist/'
out_dir: '/pscratch/sd/f/feng045/SAAG/hist/auxhist/refl/'

# Adjust hardware parameters if not using AMD EPYC 7763
chunk_size: 14  # Reduce for smaller systems
max_concurrent_chunks: 8  # Reduce for smaller systems
```

### 2. Run the main processing script

```bash
python subset_vars_from_aux.py config_subset_aux_goamazon_chunked.yml
```

### 3. Monitor progress (optional, in another terminal)

```bash
# Check current status
python monitor_progress.py /pscratch/sd/f/feng045/SAAG/hist/auxhist/refl 2016

# Watch continuously (updates every 60 seconds)
python monitor_progress.py /pscratch/sd/f/feng045/SAAG/hist/auxhist/refl 2016 --watch
```

### 4. Check for any missing files after completion

```bash
# Check for missing timestep files
python find_missing_timesteps.py config_subset_aux_goamazon_chunked.yml
```

**If any files are missing**, the script will automatically generate a resume script:

```bash
# Run the auto-generated resume script
python resume_missing_timesteps.py
```

### 5. Run on SLURM HPC system

```bash
# Edit run_subset_slurm.sh to set your account and adjust parameters
# Then submit:
sbatch run_subset_slurm.sh
```

## Memory Management Strategy

The improved script implements several strategies optimized for AMD EPYC hardware:

1. **Chunking**: Processes files in 2-week chunks (14 daily file pairs) rather than all at once
2. **Threaded Workers**: Uses threads instead of processes to avoid multiprocessing bootstrap errors
3. **Memory Limits**: Sets conservative per-worker memory limits (8GB) with spill/terminate thresholds
4. **NUMA Awareness**: Optimized worker configuration for 4 NUMA domains on AMD EPYC
5. **Progress Tracking**: Provides detailed logging and error handling
6. **Conservative Cleanup**: Proper cleanup of Dask client and cluster

## Troubleshooting

### If processing stops or crashes:

1. **Check completion status**:
   ```bash
   python find_missing_timesteps.py config_subset_aux_goamazon_chunked.yml
   ```

2. **If files are missing**, the script will auto-generate a resume script:
   ```bash
   python resume_missing_timesteps.py
   ```

3. **For persistent issues**, reduce parallelism:
   ```yaml
   chunk_size: 7  # Reduce from 14 to 7 (1 week chunks)
   max_concurrent_chunks: 4  # Reduce from 8 to 4
   ```

### Memory issues:

- **Out of memory**: Reduce `max_concurrent_chunks` (each chunk uses ~10GB)
- **Slow performance**: Increase `chunk_size` to reduce overhead
- **Multiprocessing errors**: The script now uses threads (fixed)

### Performance tuning:

- **More speed**: Increase `max_concurrent_chunks` if you have sufficient memory
- **Less memory**: Decrease `chunk_size` or `max_concurrent_chunks`
- **Different hardware**: Adjust `nprocesses` and memory limits in the script

## Data Flow

```
Input: 365 daily files (auxhist_d01_YYYY-MM-DD_00:00:00)
   ↓
File pairs: 364 pairs of adjacent daily files ([day1,day2], [day2,day3], ...)
   ↓  
Chunks: 26 chunks of 14 daily file pairs each (2-week chunks)
   ↓
Batch processing: 8 concurrent chunks maximum
   ↓
Output: 35,039 individual timestep files (refl_YYYY-MM-DD_HH_MM_SS.nc)
```

Note: The first timestep (00:00:00) of the first day is excluded because it has no precipitation rate data.

## Expected Results

For a full year of processing, you should expect:

**File counts**:
- Input: 365 daily files
- File pairs: 364 pairs  
- Output: 35,039 timestep files (365 days × 96 timesteps - 1 first timestep)

**Processing time** (AMD EPYC 7763):
- Full year: ~4-6 hours (depending on I/O performance)
- Resume script (if needed): ~30 minutes for small number of missing files

**Completion verification**:
```bash
python find_missing_timesteps.py config_subset_aux_goamazon_chunked.yml
# Should show: "All timestep files are present - no reprocessing needed!"
```

## Important Notes

1. **Variable name typo**: The config file contains `out_basenbame` (with typo) instead of `out_basename` - this is intentional and the scripts handle it correctly.

2. **Path construction**: The improved `find_missing_timesteps.py` handles the trailing slash in `out_dir` correctly to avoid double slash issues.

3. **Multiprocessing fix**: The scripts use `processes=False` in LocalCluster to avoid Python multiprocessing bootstrap errors on HPC systems.

4. **File pair logic**: The processing creates 364 file pairs from 365 daily files, where each pair processes timesteps from both days to handle the 15-minute offset properly.

This approach should handle the full year of data without crashing by managing memory usage optimally for AMD EPYC hardware and providing reliable recovery mechanisms. The 2-week chunking approach processes ~8 concurrent chunks instead of trying to handle all 364 daily file pairs at once, making it much more stable for HPC environments.
