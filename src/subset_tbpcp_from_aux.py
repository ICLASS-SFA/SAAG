import numpy as np
import time
import os, sys, glob
import logging
import xarray as xr
import pandas as pd
import dask
from dask.distributed import Client, LocalCluster
from pyflextrkr.ft_regrid_func import make_weight_file, make_grid4regridder

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
def write_netcdf(
        _TimeStr,
        _basetime,
        _rainrate,
        _tb,
        fileout,
        x_coord_out,
        y_coord_out,
):
    """
    Write output to netCDF file.

    Args:
        _TimeStr: string
            Time string for output file.
        _basetime: np.float
            Base time value in Epoch.
        _rainrate: np.array
            Rain rate array.
        _tb: np.array
            Tb array.
        fileout: string
            Output filename.
        x_coord_out: np.array
            X coordinate array.
        y_coord_out: np.array
            Y coordinate array.

    Return:
        None.
    """
    # # Get parameters from config
    # time_dimname = config.get('time_dimname', 'time')
    # x_dimname = config.get('x_dimname', 'lon')
    # y_dimname = config.get('y_dimname', 'lat')
    # x_coordname = config.get('x_coordname', 'longitude')
    # y_coordname = config.get('y_coordname', 'latitude')
    # tb_varname = config.get('tb_varname', 'tb')
    # pcp_varname = config.get('pcp_varname', 'rainrate')
    # Define xarray dataset
    var_dict = {
        # 'Times': ([time_dimname,'char'], times_char_t1),
        x_coordname: ([y_dimname, x_dimname], x_coord_out),
        y_coordname: ([y_dimname, x_dimname], y_coord_out),
        tb_varname: ([time_dimname, y_dimname, x_dimname], np.expand_dims(_tb, axis=0)),
        pcp_varname: ([time_dimname, y_dimname, x_dimname], np.expand_dims(_rainrate, axis=0)),
    }
    coord_dict = {
        time_dimname: ([time_dimname], np.expand_dims(_basetime, axis=0)),
        # 'char': (['char'], np.arange(0, strlen_t1)),
    }
    gattr_dict = {
        'Title': 'WRF calculated rainrate and brightness temperature',
        'Contact': 'Zhe Feng: zhe.feng@pnnl.gov',
        'Institution': 'Pacific Northwest National Laboratory',
        'created on': time.ctime(time.time()),
        # 'Original_File1': filein_t1,
        # 'Original_File2': filein_t2,
        # 'DX': DX,
        # 'DY': DY,
    }
    dsout = xr.Dataset(var_dict, coords=coord_dict, attrs=gattr_dict)
    # Specify attributes
    dsout[time_dimname].attrs['long_name'] = 'Epoch time (seconds since 1970-01-01 00:00:00)'
    dsout[time_dimname].attrs['units'] = 'seconds since 1970-01-01 00:00:00'
    # dsout[time_dimname].attrs['_FillValue'] = np.NaN
    dsout[time_dimname].attrs['tims_string'] = _TimeStr
    # dsout['Times'].attrs['long_name'] = 'WRF-based time'
    dsout[x_coordname].attrs['long_name'] = 'Longitude'
    dsout[x_coordname].attrs['units'] = 'degrees_east'
    dsout[y_coordname].attrs['long_name'] = 'Latitude'
    dsout[y_coordname].attrs['units'] = 'degrees_north'
    dsout[tb_varname].attrs['long_name'] = 'Brightness temperature'
    dsout[tb_varname].attrs['units'] = 'K'
    dsout[pcp_varname].attrs['long_name'] = 'Precipitation rate'
    dsout[pcp_varname].attrs['units'] = 'mm hr-1'
    # Write to netcdf file
    encoding_dict = {
        time_dimname: {'zlib': True, 'dtype': 'float'},
        # 'Times':{'zlib':True},
        x_coordname: {'zlib': True, 'dtype': 'float32'},
        y_coordname: {'zlib': True, 'dtype': 'float32'},
        tb_varname: {'zlib': True, 'dtype': 'float32'},
        pcp_varname: {'zlib': True, 'dtype': 'float32'},
    }
    dsout.to_netcdf(path=fileout, mode='w', format='NETCDF4', unlimited_dims=time_dimname, encoding=encoding_dict)
    return

#-------------------------------------------------------------------------------------
def calc_rainrate_tb(filepairnames, out_dir, in_basename, out_basename):
    """
    Calculates rain rates from a pair of WRF output files and write to netCDF

    Args:
        filepairnames: list
            A list of filenames in pair
        out_dir: string
            Output file directory.
        in_basename: string
            Input file basename.
        out_basename: string
            Output file basename.

    Returns:
        status: 0 or 1
            Returns status = 1 if success.
    """

    # regrid_input = config.get('regrid_input', False)
    # write_native = config.get('write_native', False)

    # Filenames with full path
    filein_t1 = filepairnames[0]
    print(f'Reading input f1: {filein_t1}')

    filein_t2 = filepairnames[1]
    print(f'Reading input f2: {filein_t2}')

    # # Get filename
    # fname_t1 = os.path.basename(filein_t1)
    # fname_t2 = os.path.basename(filein_t2)

    # # Get basename string position
    # idx0 = fname_t1.find(in_basename)
    # ftime_t2 = fname_t2[idx0+len(in_basename):]
    # # Output filename
    # fileout = f'{out_dir}/{out_basename}{ftime_t2}.nc'

    # # Output for native resolution
    # if (regrid_input) & (write_native):
    #     out_dir_native = f'{out_dir}/native/'
    #     fileout_native = f'{out_dir_native}/{out_basename}{ftime_t2}.nc'
    #     # Create output directory
    #     os.makedirs(out_dir_native, exist_ok=True)

    # Read in map file
    dsmap = xr.open_dataset(map_file).squeeze()
    XLONG = dsmap['XLONG']
    XLAT = dsmap['XLAT']
    ny, nx = np.shape(XLAT)
    DX = dsmap.attrs['DX']
    DY = dsmap.attrs['DY']

    # Read in WRF data files
    ds_in = xr.open_mfdataset([filein_t1, filein_t2], concat_dim='Time', combine='nested')
    # Rename 'Time' dimension to 'time'
    ds_in = ds_in.rename({'Time':'time'})
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
    ds_in['time'] = dt64
    RAINNC = ds_in['RAINNC']
    I_RAINNC = ds_in['I_RAINNC']

    # The total precipitation accumulation from the initial time is computed (units: mm)
    TOTAL_RAIN = RAINNC + I_RAINNC * 100
    # For 15-min precipitation amount, take a difference between two output times
    RAINRATE = TOTAL_RAIN.diff(dim='time')
    # Use resample to sum rainrate within each desired frequency
    RAINRATE_rs = RAINRATE.resample(time=subset_freq).sum()
    # Use resample to select OLR at desired frequency
    OLR_rs = ds_in['OLR'].resample(time=subset_freq).nearest()

    # Convert OLR to IR brightness temperature
    tb_rs = olr_to_tb(OLR_rs)
    # tb_out = olr_to_tb(OLR_out)

    # Output time stamp
    time_out = OLR_rs.time
    ntimes_out = len(time_out)

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
        # Save rainrate at time=i to time stamp time=i+1
        _tb = tb_rs[tt,:,:].data
        _rainrate = RAINRATE_rs[tt,:,:].data
        # Output filename
        _TimeStr = _time_out.dt.strftime('%Y-%m-%d_%H_%M_%S').item()
        filename_out = f'{_out_dir}{out_basenbame}{_TimeStr}.nc'

        # Write output to file
        write_netcdf(_TimeStr, _basetime, _rainrate, _tb,
                     filename_out, XLONG.data, XLAT.data)
        print(f'{filename_out}')

        # import pdb; pdb.set_trace()
    return


if __name__=='__main__':
    
    start_year = 2000
    end_year = 2000

    # Parallel setup
    run_parallel = 1
    nprocesses = 32

    # Subset time frequency
    subset_freq = '1H'

    root_dir = '/pscratch/sd/f/feng045/SAAG/hist/auxhist/'
    out_dir = f'{root_dir}/tb_pcp/'
    in_basename = 'auxhist_d01_'
    out_basenbame = 'tb_rainrate_'
    dask_tmp_dir = '/tmp'
    # Constant file
    map_file = f'/pscratch/sd/f/feng045/SAAG/hist/wrfconstants_SAAG_20yr.nc'

    # Get parameters from config
    # Define output dimension, coordinate, variable names
    time_dimname = 'time'
    x_dimname = 'lon'
    y_dimname = 'lat'
    x_coordname = 'longitude'
    y_coordname = 'latitude'
    tb_varname = 'tb'
    pcp_varname = 'rainrate'

    # Find all input files
    filelist = []
    for iyear in range(start_year, end_year+1):
        files = sorted(glob.glob(f'{root_dir}/{iyear}/{in_basename}*'))
        filelist.extend(files)
    nfiles = len(filelist)
    print(f'Number of WRF files: {nfiles}')

    # # For testing
    # filelist = filelist[0:16]
    # nfiles = len(filelist)

    # Create a list with a pair of WRF filenames that are adjacent in time
    filepairlist = []
    for ii in range(0, nfiles-1):
        ipair = [filelist[ii], filelist[ii+1]]
        filepairlist.append(ipair)
    nfilepairs = len(filepairlist)

    if run_parallel == 1:
        # Set Dask temporary directory for workers
        dask.config.set({'temporary-directory': dask_tmp_dir})
        # Local cluster
        cluster = LocalCluster(n_workers=nprocesses, threads_per_worker=1)
        client = Client(cluster)

    if run_parallel == 0:
        # Serial version
        for ifile in range(0, nfilepairs):
            status = calc_rainrate_tb(filepairlist[ifile], out_dir, in_basename, out_basenbame)

    elif run_parallel >= 1:
        # Parallel
        results = []
        for ifile in range(0, nfilepairs):
            result = dask.delayed(calc_rainrate_tb)(filepairlist[ifile], out_dir, in_basename, out_basenbame)
            results.append(result)
        final_result = dask.compute(*results)
    else:
        sys.exit('Valid parallelization flag not provided')

    import pdb; pdb.set_trace()