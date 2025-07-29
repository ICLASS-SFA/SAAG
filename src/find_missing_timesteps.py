#!/usr/bin/env python3
"""
Improved script to find missing individual timestep files
This checks for missing output files at the timestep level, not just file pair level
"""
import os
import glob
import pandas as pd
import yaml
import argparse
from collections import defaultdict

def find_missing_timestep_files(config_file):
    """Find missing individual timestep output files"""
    
    # Read configuration
    with open(config_file, 'r') as stream:
        config = yaml.full_load(stream)
    
    startdate = config.get("startdate")
    enddate = config.get("enddate")
    out_dir = config.get("out_dir")
    out_basename = config.get("out_basenbame")
    
    # Parse start and end dates
    date_format = '%Y%m%d.%H%M'
    start_dt = pd.to_datetime(startdate, format=date_format)
    end_dt = pd.to_datetime(enddate, format=date_format)
    
    # Generate expected output file list (every 15 minutes)
    # Note: We exclude the first timestep as it doesn't have precipitation rate
    expected_times = pd.date_range(start_dt + pd.Timedelta(minutes=15), end_dt, freq='15min')
    
    # Get year for output directory
    year = start_dt.year
    output_dir = f'{out_dir.rstrip("/")}/{year}/'
    
    print(f"Checking for missing files in: {output_dir}")
    print(f"Expected timesteps: {len(expected_times)} (every 15 minutes)")
    print(f"Time range: {expected_times[0]} to {expected_times[-1]}")
    
    # Check which files exist
    existing_files = set()
    missing_files = []
    
    for time in expected_times:
        time_str = time.strftime('%Y-%m-%d_%H_%M_%S')
        expected_file = f'{output_dir}{out_basename}{time_str}.nc'
        
        if os.path.exists(expected_file):
            existing_files.add(expected_file)
        else:
            missing_files.append((time, expected_file))
    
    print(f"\nResults:")
    print(f"  Existing files: {len(existing_files)}")
    print(f"  Missing files: {len(missing_files)}")
    print(f"  Completion: {len(existing_files)/len(expected_times)*100:.1f}%")
    
    if missing_files:
        # Group missing files by date
        missing_by_date = defaultdict(list)
        for time, filepath in missing_files:
            date_str = time.strftime('%Y-%m-%d')
            missing_by_date[date_str].append(time.strftime('%H:%M:%S'))
        
        print(f"\nMissing files by date:")
        for date, times in sorted(missing_by_date.items()):
            print(f"  {date}: {len(times)} files ({', '.join(times[:5])}{'...' if len(times) > 5 else ''})")
        
        # Analyze patterns
        print(f"\nMissing file patterns:")
        
        # Check for missing days (all 96 timesteps missing)
        complete_missing_days = [date for date, times in missing_by_date.items() if len(times) == 96]
        if complete_missing_days:
            print(f"  Complete missing days ({len(complete_missing_days)}): {', '.join(complete_missing_days[:5])}{'...' if len(complete_missing_days) > 5 else ''}")
        
        # Check for partially missing days
        partial_missing_days = [(date, len(times)) for date, times in missing_by_date.items() if 0 < len(times) < 96]
        if partial_missing_days:
            print(f"  Partially missing days ({len(partial_missing_days)}):")
            for date, count in partial_missing_days[:10]:
                print(f"    {date}: {count}/96 files missing")
            if len(partial_missing_days) > 10:
                print(f"    ... and {len(partial_missing_days) - 10} more")
    
    return missing_files, existing_files, config

def create_detailed_resume_script(missing_files, config, output_script):
    """Create resume script that processes specific missing timesteps"""
    
    if not missing_files:
        print("No missing files found - processing is complete!")
        return
    
    # Group missing timesteps by the daily file pairs that need to be reprocessed
    # We need to figure out which file pairs to reprocess based on missing timesteps
    
    missing_by_date = defaultdict(list)
    for time, filepath in missing_files:
        date_str = time.strftime('%Y-%m-%d')
        missing_by_date[date_str].append(time)
    
    # For each date with missing files, we need to determine the file pair to reprocess
    file_pairs_to_reprocess = set()
    
    root_dir = config.get("root_dir")
    in_basename = config.get("in_basename")
    year = list(missing_by_date.keys())[0][:4]  # Get year from first missing date
    
    for date_str, missing_times in missing_by_date.items():
        date_obj = pd.to_datetime(date_str)
        
        # The missing timesteps could come from processing either:
        # 1. The file pair [date-1, date] (for early timesteps of the day)
        # 2. The file pair [date, date+1] (for late timesteps of the day)
        
        # Check both possibilities
        prev_date = date_obj - pd.Timedelta(days=1)
        next_date = date_obj + pd.Timedelta(days=1)
        
        # File pair [prev_date, date]
        file1_prev = f"{root_dir}{year}/{in_basename}{prev_date.strftime('%Y-%m-%d_%H:%M:%S')}"
        file2_date = f"{root_dir}{year}/{in_basename}{date_obj.strftime('%Y-%m-%d_%H:%M:%S')}"
        if os.path.exists(file1_prev) and os.path.exists(file2_date):
            file_pairs_to_reprocess.add((file1_prev, file2_date))
        
        # File pair [date, next_date]
        file1_date = f"{root_dir}{year}/{in_basename}{date_obj.strftime('%Y-%m-%d_%H:%M:%S')}"
        file2_next = f"{root_dir}{year}/{in_basename}{next_date.strftime('%Y-%m-%d_%H:%M:%S')}"
        if os.path.exists(file1_date) and os.path.exists(file2_next):
            file_pairs_to_reprocess.add((file1_date, file2_next))
    
    file_pairs_list = list(file_pairs_to_reprocess)
    
    script_content = f'''#!/usr/bin/env python3
"""
Auto-generated resume script for processing missing timestep files
Generated on: {pd.Timestamp.now()}
Missing timestep files: {len(missing_files)}
File pairs to reprocess: {len(file_pairs_list)}
"""
import sys
import os
import multiprocessing
sys.path.append('{os.path.dirname(config.get("_config_file", ""))}')

from subset_vars_from_aux import process_chunk
import yaml
import dask
from dask.distributed import Client, LocalCluster

def main():
    # Load original config
    config_file = '{config.get("_config_file", "config.yml")}'
    with open(config_file, 'r') as stream:
        config = yaml.full_load(stream)

    # Conservative settings for reprocessing
    config['chunk_size'] = 5  # Small chunks for stability
    config['max_concurrent_chunks'] = 2  # Very conservative

    # File pairs to reprocess
    file_pairs_to_reprocess = {repr(file_pairs_list)}

    print(f"Reprocessing {{len(file_pairs_to_reprocess)}} file pairs to generate {{len(file_pairs_to_reprocess) * 96}} timestep files...")
    print("Note: This will overwrite existing files for these dates")

    # Setup Dask cluster
    nprocesses = config.get("nprocesses", 128)  # Use full CPU allocation
    dask_tmp_dir = config.get("dask_tmp_dir", "/tmp")

    # Set Dask temporary directory
    dask.config.set({{'temporary-directory': dask_tmp_dir}})
    # Optimized configuration for AMD EPYC hardware (same as main processing)
    dask.config.set({{
        'array.chunk-size': '128MB',  # Larger chunks for high-memory system
        'distributed.worker.memory.target': 0.75,  # More conservative for stability
        'distributed.worker.memory.spill': 0.85,
        'distributed.worker.memory.pause': 0.9,
        'distributed.worker.memory.terminate': 0.95,
        'distributed.scheduler.allowed-failures': 3,  # Allow some task failures
        'distributed.scheduler.retry-policy': True,
    }})

    # Optimize cluster for AMD EPYC with high core count and memory
    # Use fewer workers with more memory each for better efficiency
    n_workers = min(nprocesses // 2, 32)  # Use up to 32 workers, with 2+ cores each
    threads_per_worker = max(1, nprocesses // n_workers)
    memory_per_worker = min(15, 512 // n_workers)  # Up to 15GB per worker
    
    cluster = LocalCluster(
        n_workers=n_workers, 
        threads_per_worker=threads_per_worker,
        memory_limit=f'{{memory_per_worker}}GB',
        dashboard_address=':8787',
        processes=False,  # Keep threads for multiprocessing compatibility
        silence_logs=False,  # Keep logs for debugging
    )
    client = Client(cluster)
    print(f"Dask cluster: {{n_workers}} workers × {{threads_per_worker}} threads = {{n_workers * threads_per_worker}} total cores")
    print(f"Memory per worker: {{memory_per_worker}}GB, Total memory: {{n_workers * memory_per_worker}}GB")
    print(f"Dask dashboard: {{client.dashboard_link}}")

    # Process in small chunks
    chunk_size = config['chunk_size']
    chunks = []
    for i in range(0, len(file_pairs_to_reprocess), chunk_size):
        chunk = file_pairs_to_reprocess[i:i + chunk_size]
        chunks.append(chunk)

    print(f"Processing {{len(chunks)}} chunks...")

    total_processed = 0
    total_failed = 0

    for batch_start in range(0, len(chunks), config['max_concurrent_chunks']):
        batch_end = min(batch_start + config['max_concurrent_chunks'], len(chunks))
        batch_chunks = chunks[batch_start:batch_end]
        
        print(f"Processing batch {{batch_start//config['max_concurrent_chunks'] + 1}}")
        
        # Create delayed tasks
        batch_results = []
        for chunk_id, chunk in enumerate(batch_chunks, start=batch_start):
            result = dask.delayed(process_chunk)(chunk, config, chunk_id)
            batch_results.append(result)
        
        # Compute batch
        batch_outputs = dask.compute(*batch_results)
        
        # Aggregate results
        for chunk_id, n_processed, n_failed in batch_outputs:
            total_processed += n_processed
            total_failed += n_failed
        
        print(f"Batch completed. Running totals: {{total_processed}} processed, {{total_failed}} failed")

    print(f"Reprocessing completed: {{total_processed}} file pairs processed, {{total_failed}} failed")

    # Close cluster
    client.close()
    cluster.close()

    print("\\nRerun the check script to verify all files are now present:")
    print("python find_missing_timesteps.py your_config.yml")

if __name__ == '__main__':
    multiprocessing.freeze_support()  # For Windows compatibility
    main()
'''
    
    with open(output_script, 'w') as f:
        f.write(script_content)
    
    os.chmod(output_script, 0o755)  # Make executable
    print(f"\\nCreated timestep-aware resume script: {output_script}")
    print(f"This script will reprocess {len(file_pairs_list)} file pairs")
    print(f"Run with: python {output_script}")

def main():
    parser = argparse.ArgumentParser(description='Find missing timestep files and create resume script')
    parser.add_argument('config_file', help='Original config file path')
    parser.add_argument('--output-script', default='resume_missing_timesteps.py', 
                       help='Output script name (default: resume_missing_timesteps.py)')
    
    args = parser.parse_args()
    
    # First, find missing files (this reads config fresh)
    missing_files, existing_files, config = find_missing_timestep_files(args.config_file)

    # Store absolute path of config file for script generation AFTER getting config
    config_file_path = os.path.abspath(args.config_file)
    config['_config_file'] = config_file_path
    
    if missing_files:
        create_detailed_resume_script(missing_files, config, args.output_script)
    else:
        print("\\nAll timestep files are present - no reprocessing needed!")

if __name__ == '__main__':
    main()
