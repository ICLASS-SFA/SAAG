# SAAG Processing Scripts - Cleanup Notes

## Completed Cleanup (July 29, 2025)

The following files have been removed as they are no longer necessary:

### Removed Files:
- `resume_missing_timesteps.py` - Auto-generated resume script (completed successfully)
- `find_missing_and_resume.py` - Original file-pair-level missing file detection (had bugs)
- `subset_vars_from_aux_backup.py` - Backup copy (no longer needed)
- `debug_file_pair.py` - Single file pair testing script
- `deleteme/` - Entire directory with obsolete scripts
- `*.err`, `*.out` - SLURM output files
- `tasks_subset_*.txt` - Old task files for array jobs
- `config_subset_aux_goamazon_aggressive.yml` - Obsolete config
- `config_subset_aux_goamazon_chunked_40889926.yml` - Job-specific config copy

## Scripts Kept (Essential for processing)

### Core Processing
- `subset_vars_from_aux.py` - Main processing script ✅
- `config_subset_aux_goamazon_chunked.yml` - Optimized configuration ✅
- `find_missing_timesteps.py` - Improved missing file detection ✅

### SLURM Job Management
- `slurm_subset_vars_from_aux.sh` - SLURM job script ✅

### Monitoring
- `monitor_progress.py` - Progress monitoring script ✅

### Documentation
- `README_improved_subsetting.md` - Updated usage documentation ✅
- `CLEANUP_NOTES.md` - This file ✅
- `PERFORMANCE_TUNING.md` - Performance optimization notes ✅

## Cleanup Commands

To remove the unnecessary files:

```bash
# Remove auto-generated resume script (after successful completion)
rm resume_missing_timesteps.py

# Remove obsolete scripts
rm find_missing_and_resume.py
rm resume_processing.py

# Optional: Remove debug script (or keep for future debugging)
# rm debug_file_pair.py
```

## For Processing Future Years

You only need these files:
1. `subset_vars_from_aux.py` - Main script
2. `config_subset_aux_goamazon_chunked.yml` - Edit dates for target year
3. `find_missing_timesteps.py` - Check completion and auto-generate resume script if needed
4. `monitor_progress.py` - Monitor progress (optional)
5. `README_improved_subsetting.md` - Usage instructions

The workflow is:
1. Edit config file for target year
2. Run main processing script
3. Check completion with find_missing_timesteps.py
4. If needed, run auto-generated resume script
