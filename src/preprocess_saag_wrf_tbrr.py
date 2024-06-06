"""
Convert SAAG WRF regridded OLR to Tb, combine with PREC and split monthly files to hourly files.
Use Xarray and Dask for parallel processing.
"""
__author__ = "Zhe.Feng@pnnl.gov"
__date__ = "16-Mar-2022"

import numpy as np
import glob, os
import xarray as xr
import pandas as pd
import time, datetime, calendar, pytz
import dask
from dask import delayed
from dask.distributed import Client, LocalCluster

#-----------------------------------------------------------------------
def subset_data(infile_olr, infile_pcp, outdir, outbasename, compute=True, first_date=None):

    # Read input file
    drop_varlist = []
    print('Reading input data ... ')
    dsolr = xr.open_dataset(infile_olr, drop_variables=drop_varlist, decode_times=False)
    dspcp = xr.open_dataset(infile_pcp, drop_variables=drop_varlist, decode_times=False)

    # Replace the incorrect time units
    # dsolr['time'].attrs['units'] = f"hours since {first_date} 00:00:00"
    # Decode the time
    dsolr = xr.decode_cf(dsolr)
    time_orig = dsolr['time']
    # Round the times to hourly
    dsolr['time'] = time_orig.dt.round('H')

    # Replace precipitation dataset time, add PREC_ACC_NC to dsolr
    dspcp['time'] = dsolr['time']
    dsolr['rainrate'] = dspcp['PREC_ACC_NC']

    # Calculate brightness temperature
    # (1984) as given in Yang and Slingo (2001)
    # Tf = tb(a+b*Tb) where a = 1.228 and b = -1.106e-3 K^-1
    # OLR = sigma*Tf^4 
    # where sigma = Stefan-Boltzmann constant = 5.67x10^-8 W m^-2 K^-4
    a = 1.228
    b = -1.106e-3
    sigma = 5.67e-8 # W m^-2 K^-4
    tf = (dsolr['LWUPT']/sigma)**0.25
    tb = (-a + np.sqrt(a**2 + 4*b*tf))/(2*b)

    # Replace unphysical value (missing) with NAN
    # tb[tb < 10] = np.NAN
    tb = tb.where(tb > 10, other=np.NAN)

    # Replace OLR with Tb
    dsolr['LWUPT'] = tb
    dsolr = dsolr.rename({'LWUPT':'tb'})
    # Add attributes
    dsolr['tb'].attrs['standard_name'] = 'Brightness temperature'
    dsolr['tb'].attrs['long_name'] = 'Infrared brightness temperature converted from LWUPT following Yang and Slingo (2001)'
    dsolr['tb'].attrs['units'] = 'K'
    
    # Resample to hourly
    dates, datasets = zip(*dsolr.resample(time='1H'))
    # Get number of times
    ndates = len(datasets)
    # Create a list of output filenames
    # Output file datetime format: yyyy-mm-dd_hh:mm.nc
    filenames_out = []
    for idate in range(0, ndates):
        filenames_out.append(outdir + outbasename + datasets[idate].indexes['time'].strftime('%Y-%m-%d_%H:%M')[0] + '.nc')

    # Write to netCDF
    print("Writing out files ... ")
    dsout = xr.save_mfdataset(datasets, filenames_out, compute=compute)
    print("Done writing.")

    return dsout

#-----------------------------------------------------------------------
def work_on_file(
    filesin_olr, filesin_pcp, outdir, outbasename, 
    run_parallel=0, n_workers=1, first_date=None,
):

    if run_parallel==0:
        dsout = subset_data(filesin_olr, filesin_pcp, outdir, outbasename, compute=True, first_date=first_date)

    elif run_parallel==1:
        dsout = subset_data(filesin_olr, filesin_pcp, outdir, outbasename, compute=False, first_date=first_date)
        dsout.compute()

#-----------------------------------------------------------------------
if __name__=='__main__':

    # Set flag to run in parallel (1) or serial (0)
    run_parallel = 1
    # Number of workers for Dask
    n_workers = 32
    
    start_datetime = '2010-06-01'
    end_datetime = '2011-06-01'
    # first_date = '2015-06-01'

    rootdir = '/gpfs/wolf/atm123/proj-shared/zhefeng/SAAG/'
    # runnames = ['June2018_May2019_nsp']
    # runnames = ['June2015_June2016_nsp']
    runnames = ['June2010_June2011.nsp']
    rootoutdir = rootdir
    outbasename = 'tb_rainrate_'

    if run_parallel==1:
        # Set Dask temporary directory for workers
        dask_tmp_dir = "/tmp"
        dask.config.set({'temporary-directory': dask_tmp_dir})
            
        # Initialize dask
        cluster = LocalCluster(n_workers=n_workers)
        client = Client(cluster)

    # Loop over each run
    for runname in runnames:
        # Define input/output directory
        datadir = f'{rootdir}{runname}/'
        outdir = f'{rootoutdir}{runname}/tb_rr/'
        basename1 = '_LWUPT_SouthAmerica_on-IMERG-grid'
        basename2 = '_PREC_ACC_NC_SouthAmerica_on-IMERG-grid'
        # Create output directory
        os.makedirs(outdir, exist_ok=True)

        # Generate time marks within the start/end datetime
        file_datetimes = pd.date_range(start=start_datetime, end=end_datetime, freq='1M').strftime('%Y%m')
        # Find input files
        files_olr = []
        files_pcp = []
        for tt in range(0, len(file_datetimes)):
            files_olr.extend(sorted(glob.glob(f'{datadir}{file_datetimes[tt]}*{basename1}*nc')))
            files_pcp.extend(sorted(glob.glob(f'{datadir}{file_datetimes[tt]}*{basename2}*nc')))

        # Check matching number of OLR and RAIN files
        nfiles_olr = len(files_olr)
        nfiles_pcp = len(files_pcp)
        if (nfiles_olr != nfiles_pcp):
            print(f'Number of files not matching!')
        else:
            print(f'Number of files: {nfiles_olr}')

        # Loop over each month to process
        for ifile in range(0, nfiles_olr):
            print(files_olr[ifile])
            work_on_file(
                files_olr[ifile], files_pcp[ifile], outdir, outbasename, \
                run_parallel=run_parallel, n_workers=n_workers, 
            )
