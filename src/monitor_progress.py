#!/usr/bin/env python3
"""
Monitor the progress of subset_vars_from_aux.py processing
Checks output directory for completed files and reports statistics
"""
import os
import glob
import pandas as pd
import argparse
from datetime import datetime, timedelta

def count_output_files(output_dir, year):
    """Count output files by month and day"""
    pattern = f"{output_dir}/{year}/refl_*.nc"
    files = glob.glob(pattern)
    
    if not files:
        print(f"No output files found in {output_dir}/{year}/")
        return
    
    # Parse timestamps from filenames
    timestamps = []
    for f in files:
        basename = os.path.basename(f)
        # Extract timestamp: refl_YYYY-MM-DD_HH_MM_SS.nc
        try:
            timestamp_str = basename.replace('refl_', '').replace('.nc', '')
            timestamp_str = timestamp_str.replace('_', ':')  # Convert to YYYY-MM-DD:HH:MM:SS
            timestamp_str = timestamp_str.replace(':', '-', 2)  # Convert to YYYY-MM-DD-HH:MM:SS
            timestamp_str = timestamp_str.replace('-', ':', 3)  # Convert to YYYY-MM-DD:HH:MM:SS
            timestamp = pd.to_datetime(timestamp_str, format='%Y-%m-%d:%H:%M:%S')
            timestamps.append(timestamp)
        except:
            print(f"Could not parse timestamp from {basename}")
            continue
    
    if not timestamps:
        print("No valid timestamps found in output files")
        return
    
    # Create DataFrame for analysis
    df = pd.DataFrame({'timestamp': timestamps})
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    df['month'] = df['timestamp'].dt.month
    
    print(f"\nOutput file statistics for {year}:")
    print(f"Total files: {len(files)}")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Files per month
    print("\nFiles per month:")
    monthly_counts = df.groupby('month').size()
    for month, count in monthly_counts.items():
        month_name = pd.to_datetime(f'{year}-{month:02d}-01').strftime('%B')
        print(f"  {month_name}: {count}")
    
    # Expected vs actual files
    start_date = pd.to_datetime(f'{year}-01-01')
    end_date = pd.to_datetime(f'{year}-12-31 23:45:00')  # Last 15-min interval of year
    expected_intervals = pd.date_range(start_date, end_date, freq='15min')
    expected_count = len(expected_intervals) - 1  # -1 because we need pairs
    
    print(f"\nExpected file pairs: {expected_count}")
    print(f"Actual files: {len(files)}")
    print(f"Completion: {len(files)/expected_count*100:.1f}%")
    
    # Find missing days
    daily_counts = df.groupby('date').size()
    expected_daily = 96  # 24 hours * 4 (15-min intervals)
    
    missing_days = []
    incomplete_days = []
    
    for date in pd.date_range(start_date.date(), end_date.date()):
        if date not in daily_counts.index:
            missing_days.append(date)
        elif daily_counts[date] < expected_daily:
            incomplete_days.append((date, daily_counts[date]))
    
    if missing_days:
        print(f"\nMissing days ({len(missing_days)}):")
        for day in missing_days[:10]:  # Show first 10
            print(f"  {day}")
        if len(missing_days) > 10:
            print(f"  ... and {len(missing_days)-10} more")
    
    if incomplete_days:
        print(f"\nIncomplete days ({len(incomplete_days)}):")
        for day, count in incomplete_days[:10]:  # Show first 10
            print(f"  {day}: {count}/{expected_daily} files")
        if len(incomplete_days) > 10:
            print(f"  ... and {len(incomplete_days)-10} more")
    
    return len(files), expected_count, len(missing_days), len(incomplete_days)

def main():
    parser = argparse.ArgumentParser(description='Monitor subset processing progress')
    parser.add_argument('output_dir', help='Output directory path')
    parser.add_argument('year', help='Year to check (e.g., 2015)')
    parser.add_argument('--watch', action='store_true', help='Watch mode - check every 60 seconds')
    
    args = parser.parse_args()
    
    if args.watch:
        print(f"Watching {args.output_dir}/{args.year}/ for progress...")
        print("Press Ctrl+C to stop")
        try:
            while True:
                print(f"\n--- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
                count_output_files(args.output_dir, args.year)
                print("\nSleeping for 60 seconds...")
                import time
                time.sleep(60)
        except KeyboardInterrupt:
            print("\nStopped monitoring")
    else:
        count_output_files(args.output_dir, args.year)

if __name__ == '__main__':
    main()
