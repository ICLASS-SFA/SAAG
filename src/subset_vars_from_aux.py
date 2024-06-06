"""
Subset SAAG auxilary files within a specified region, period and variables and write out to netCDF files
Also converts OLR to Tb and calculates rain rates.
"""
import numpy as np
import time
import os, sys, glob, yaml
import xarray as xr
import pandas as pd
import dask
from dask.distributed import Client, LocalCluster
from pyflextrkr.ft_utilities import subset_files_timerange

#-------------------------------------------------------------------------------------
def olr_to_tb(OLR):
    """
    Convert OLR to IR brightness temperature.

    Args:
        OLR: np.array
            Outgoing longwave radiation
    
    Returns:
        tb: np.array
            Brightness temperature
    """
    # Calculate brightness temperature
    # (1984) as given in Yang and Slingo (2001)
    # Tf = tb(a+b*Tb) where a = 1.228 and b = -1.106e-3 K^-1
    # OLR = sigma*Tf^4 
    # where sigma = Stefan-Boltzmann constant = 5.67x10^-8 W m^-2 K^-4
    a = 1.228
    b = -1.106e-3
    sigma = 5.67e-8 # W m^-2 K^-4
    tf = (OLR/sigma)**0.25
    tb = (-a + np.sqrt(a**2 + 4*b*tf))/(2*b)
    return tb

#-------------------------------------------------------------------------------------
def subset_vars(filepairnames, config):
    """
    Subset variables from a pair of WRF output files and write to netCDF

    Args:
        filepairnames: list
            A list of filenames in pair
        config: dictionary
            Dictionary containing config parameters

    Returns:
        status: 0 or 1
            Returns status = 1 if success.
    """

    out_dir = config.get("out_dir")
    # Map file
    map_file = config.get("map_file")
    # Subset domain
    lonmin = config.get("lonmin")
    lonmax = config.get("lonmax")
    latmin = config.get("latmin")
    latmax = config.get("latmax")

    # # Get parameters from config
    time_format = config.get("time_format")
    time_dimname = config.get("time_dimname")
    x_dimname = config.get("x_dimname")
    y_dimname = config.get("y_dimname")
    time_coordname = config.get("time_coordname")
    x_coordname = config.get("x_coordname")
    y_coordname = config.get("y_coordname")
    tb_varname = config.get("tb_varname")
    pcp_varname = config.get("pcp_varname")
    pass_varname = config.get("pass_varname", None)

    # Filenames with full path
    filein_t1 = filepairnames[0]
    print(f'Reading input f1: {filein_t1}')

    filein_t2 = filepairnames[1]
    print(f'Reading input f2: {filein_t2}')


    # Read in map file
    dsmap = xr.open_dataset(map_file).squeeze()
    XLONG = dsmap['XLONG']
    XLAT = dsmap['XLAT']
    # ny, nx = np.shape(XLAT)
    DX = dsmap.attrs['DX']
    DY = dsmap.attrs['DY']

    # Make a 2D mask for subset
    mask = ((XLONG >= lonmin) & (XLONG <= lonmax) & \
            (XLAT >= latmin) & (XLAT <= latmax)).squeeze()
    # Get y/x indices limits from the mask
    y_idx, x_idx = np.where(mask == True)
    xmin, xmax = np.min(x_idx), np.max(x_idx)
    ymin, ymax = np.min(y_idx), np.max(y_idx)
    # Subset lat/lon
    x_coord_out = XLONG.isel({'west_east':slice(xmin,xmax), 'south_north':slice(ymin,ymax)})
    y_coord_out = XLAT.isel({'west_east':slice(xmin,xmax), 'south_north':slice(ymin,ymax)})
    ny, nx = np.shape(XLAT)
    
    # Read in WRF data files
    ds_in = xr.open_mfdataset([filein_t1, filein_t2], concat_dim='Time', combine='nested')
    # Subset domain
    ds_in = ds_in.isel({'west_east':slice(xmin,xmax), 'south_north':slice(ymin,ymax)})

    # Rename 'Time' dimension to 'time'
    ds_in = ds_in.rename({'Time':time_coordname})
    # Convert time to Epoch time
    Times = ds_in['Times'].load()
    ntimes = len(Times)
    Times_str = []
    basetimes = np.full(ntimes, np.NAN, dtype=float)
    dt64 = np.empty(ntimes, dtype='datetime64[ns]')
    for tt in range(0, ntimes):
        # Decode bytes to string with UTF-8 encoding, then replace "_" with "T"
        # to make time string: YYYY-MO-DDTHH:MM:SS
        tstring = Times[tt].item().decode("utf-8").replace("_", "T")
        Times_str.append(tstring)
        # Convert to TimeStamp and save
        dt64[tt] = pd.to_datetime(tstring)
        # Convert time string to Epoch time
        basetimes[tt] = pd.to_datetime(tstring).timestamp()

    # Add time coordinate to DataSet
    ds_in[time_coordname] = dt64
    RAINNC = ds_in['RAINNC']
    I_RAINNC = ds_in['I_RAINNC']

    # The total precipitation accumulation from the initial time is computed (units: mm)
    TOTAL_RAIN = RAINNC + I_RAINNC * 100
    # For 15-min precipitation amount, take a difference between two output times
    RAINRATE = TOTAL_RAIN.diff(dim=time_dimname)
    # # Use resample to sum rainrate within each desired frequency
    # RAINRATE_rs = RAINRATE.resample(time=subset_freq).sum()
    # # Use resample to select OLR at desired frequency
    # OLR_rs = ds_in['OLR'].resample(time=subset_freq).nearest()

    # Convert OLR to IR brightness temperature
    tb = olr_to_tb(ds_in['OLR'])

    # Output time stamp
    time_out = ds_in.time
    ntimes_out = len(time_out)

    if pass_varname is not None:
        # Find the common variable names between the dataset and the list
        pass_varname = set(ds_in.data_vars) & set(pass_varname)
        # Subset the input dataset
        ds_pass = ds_in[pass_varname]

    # Output directory
    _year_str = time_out[0].dt.strftime('%Y').item()
    _out_dir = f'{out_dir}/{_year_str}/'
    # Make output directory
    os.makedirs(_out_dir, exist_ok=True)

    # Write single time frame to netCDF output
    for tt in range(0, ntimes_out-1):
        # Use the next time to be consitent with output filename
        _time_out = time_out[tt+1]
        _basetime = basetimes[tt+1]        
        _tb = tb[tt+1,:,:].load().data
        # Save rainrate at time=tt to time stamp time=tt+1
        _rainrate = RAINRATE[tt,:,:].load().data
        # Output filename
        _TimeStr = _time_out.dt.strftime('%Y-%m-%d_%H_%M_%S').item()
        filename_out = f'{_out_dir}{out_basenbame}{_TimeStr}.nc'

        # Define xarray dataset
        var_dict = {
            # 'Times': ([time_dimname,'char'], times_char_t1),
            x_coordname: ([y_dimname, x_dimname], x_coord_out.data),
            y_coordname: ([y_dimname, x_dimname], y_coord_out.data),
            tb_varname: ([time_dimname, y_dimname, x_dimname], np.expand_dims(_tb, axis=0)),
            pcp_varname: ([time_dimname, y_dimname, x_dimname], np.expand_dims(_rainrate, axis=0)),
        }
        coord_dict = {
            time_dimname: ([time_dimname], np.expand_dims(_basetime, axis=0)),
            # 'char': (['char'], np.arange(0, strlen_t1)),
        }
        gattr_dict = {
            'Title': 'WRF subset auxhist data',
            'Contact': 'Zhe Feng: zhe.feng@pnnl.gov',
            'Institution': 'Pacific Northwest National Laboratory',
            'created on': time.ctime(time.time()),
            # 'Original_File1': filein_t1,
            # 'Original_File2': filein_t2,
            'DX': DX,
            'DY': DY,
        }
        # Add pass out variables to the output variable dictionary
        if pass_varname is not None:
            # Subset the time from the pass out Dataset
            dsp = ds_pass.isel({time_coordname:tt+1})
            # Loop over each pass out variable list
            for ivar in pass_varname:
                var_dict[ivar] = ([time_dimname, y_dimname, x_dimname], np.expand_dims(dsp[ivar].data, 0), dsp[ivar].attrs)

        # Define xarray dataset
        dsout = xr.Dataset(var_dict, coords=coord_dict, attrs=gattr_dict)
        # Specify attributes
        dsout[time_dimname].attrs['long_name'] = 'Epoch time (seconds since 1970-01-01 00:00:00)'
        dsout[time_dimname].attrs['units'] = 'seconds since 1970-01-01 00:00:00'
        dsout[time_dimname].attrs['tims_string'] = _TimeStr
        dsout[x_coordname].attrs['long_name'] = 'Longitude'
        dsout[x_coordname].attrs['units'] = 'degrees_east'
        dsout[y_coordname].attrs['long_name'] = 'Latitude'
        dsout[y_coordname].attrs['units'] = 'degrees_north'
        dsout[tb_varname].attrs['long_name'] = 'Brightness temperature'
        dsout[tb_varname].attrs['units'] = 'K'
        dsout[pcp_varname].attrs['long_name'] = 'Precipitation rate'
        dsout[pcp_varname].attrs['units'] = 'mm hr-1'

        # Set encoding/compression for all variables
        comp = dict(zlib=True)
        encoding = {var: comp for var in dsout.data_vars}
        # Write to netcdf file
        dsout.to_netcdf(path=filename_out, mode='w', format='NETCDF4', unlimited_dims=time_dimname, encoding=encoding)
        print(f'{filename_out}')
    return


if __name__=='__main__':

    # Load configuration file
    config_file = sys.argv[1]

    # Read configuration from yaml file
    stream = open(config_file, "r")
    config = yaml.full_load(stream)
    
    startdate = config.get("startdate")
    enddate = config.get("enddate")

    # Parallel setup
    run_parallel = config.get("run_parallel")
    nprocesses = config.get("nprocesses")
    dask_tmp_dir = config.get("dask_tmp_dir")

    # Subset time frequency
    # subset_freq = '1H'
    root_dir = config.get("root_dir")
    # out_dir = config.get("out_dir")
    in_basename = config.get("in_basename")
    out_basenbame = config.get("out_basenbame")
    time_format = config.get("time_format")

    # Identify files to process
    in_year = startdate[0:4]
    in_dir = f'{root_dir}{in_year}/'

    # Convert start/end datetime to base time
    date_format = '%Y%m%d.%H%M'
    start_basetime = pd.to_datetime(startdate, format=date_format).timestamp()
    end_basetime = pd.to_datetime(enddate, format=date_format).timestamp()
    # Find input files within the start/end datetime
    infiles_info = subset_files_timerange(
        in_dir,
        in_basename,
        start_basetime=start_basetime,
        end_basetime=end_basetime,
        time_format=time_format,
    )
    # Get file list
    filelist = infiles_info[0]
    nfiles = len(filelist)
    print(f'Number of WRF files: {nfiles}')

    # Create a list with a pair of WRF filenames that are adjacent in time
    filepairlist = []
    for ii in range(0, nfiles-1):
        ipair = [filelist[ii], filelist[ii+1]]
        filepairlist.append(ipair)
    nfilepairs = len(filepairlist)

    # Setup Dask cluster
    if run_parallel == 1:
        # Set Dask temporary directory for workers
        dask.config.set({'temporary-directory': dask_tmp_dir})
        # Local cluster
        cluster = LocalCluster(n_workers=nprocesses, threads_per_worker=1)
        client = Client(cluster)

    if run_parallel == 0:
        # Serial version
        for ifile in range(0, nfilepairs):
            status = subset_vars(filepairlist[ifile], config)

    elif run_parallel >= 1:
        # Parallel
        results = []
        for ifile in range(0, nfilepairs):
            result = dask.delayed(subset_vars)(filepairlist[ifile], config)
            results.append(result)
        final_result = dask.compute(*results)
    else:
        sys.exit('Valid parallelization flag not provided')