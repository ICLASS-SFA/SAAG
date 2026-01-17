"""
Extracts environment profile data at CI and fixed location from WRF files for tracked convective cells.
"""
from genericpath import isfile
import numpy as np
import os, sys
import time
import yaml
import xarray as xr
import pandas as pd
import warnings
import dask
from dask.distributed import Client, LocalCluster, wait

#-----------------------------------------------------------------------
def location_to_idx(lat, lon, center):
    """ 
    Convert a latitude and longitude into an index
    
    Args:
        lat: np.array
            Latitude array
        lon: np.array
            Longitufde array
        center: tuple(float)
            location tuple to find (lat, lon)
    
    Returns:
        lat_idx: int
            Index for latitude
        lon_idx: int
            Index for longitude
    """
    # This is for 2D lat/lon
    diff = abs(lat - center[0]) + abs(lon - center[1])
    lat_idx, lon_idx = np.unravel_index(diff.argmin(), diff.shape)
    return lat_idx, lon_idx


#-----------------------------------------------------------------------
def extract_env_prof(
    fname_wrfout, 
    idx_track, 
    _lat,
    _lon,
    config,
):
    """
    Extract data center at a track.
    
    Args:
        fname_wrfout: string
            WRF out file name
        idx_track: np.array
            Track indices in the pixel file
        _lat: np.array
            Center latitude of a track
        _lon: np.array
            Center longitude of a track
        config: dictionary
            Dictionary containing config parameters

    Returns:
        out_dict3d: dictionary
            Dictionary containing the 3d track statistics data
        out_dict2d: dictionary
            Dictionary containing the 2d track statistics data
        out_dict_attrs: dictionary
            Dictionary containing the attributes of track statistics data
        out_coords: dictionary
            Dictionary containing the coordinates of track statistics data
    """
    # Get values from config
    # nx = config['nx']
    # ny = config['ny']
    nz = config.get('nz', 60)
    # sub_x = config.get('sub_x', 1)
    # sub_y = config.get('sub_y', 1)
    DX = config.get('DX')
    DY = config.get('DY')
    # geolimits = config.get('geolimits')
    lat_site = config.get('lat_site')
    lon_site = config.get('lon_site')
    xdimname = 'west_east'
    ydimname = 'south_north'
    zdimname = 'bottom_top'
    xdimname_stag = 'west_east_stag'
    ydimname_stag = 'south_north_stag'
    zdimname_stag = 'bottom_top_stag'

    row_min = config.get('row_min')
    row_max = config.get('row_max')
    col_min = config.get('col_min')
    col_max = config.get('col_max')

    # Check file existance
    wrfout_exist = os.path.isfile(fname_wrfout)
    # met_exist = os.path.isfile(fname_met)
    # cld_exist = os.path.isfile(fname_cld)
    # pixel_exist = os.path.isfile(fname_pixel)
    # import pdb; pdb.set_trace()

    # if pixel_exist:
    #     dsp = xr.open_dataset(fname_pixel)
    #     nx_p = dsp.sizes['lon']
    #     ny_p = dsp.sizes['lat']
    #     pixel_attrs = {
    #         'conv_core': dsp['conv_core'].attrs,
    #         'conv_mask': dsp['conv_mask'].attrs,
    #         # 'tracknumber_cmask': dsp['tracknumber_cmask'].attrs,
    #         'tracknumber': dsp['tracknumber'].attrs,
    #         # 'comp_ref': dsp['comp_ref'].attrs,
    #         'dbz_comp': dsp['dbz_comp'].attrs,
    #         'echotop10': dsp['echotop10'].attrs,
    #     }
    # else:
    #     pixel_attrs = {
    #         'conv_core': '',
    #         'conv_mask': '',
    #         # 'tracknumber_cmask': '',
    #         'tracknumber': '',
    #         'dbz_comp': '',
    #         'echotop10': '',
    #     }

    if wrfout_exist:
        # Read wrfout file
        print(fname_wrfout)
        # Subset to region of interest
        dsm = xr.open_dataset(fname_wrfout).isel(
            {ydimname: slice(row_min, row_max), 
             xdimname: slice(col_min, col_max),
             ydimname_stag: slice(row_min, row_max),
             xdimname_stag: slice(col_min, col_max)}).squeeze()

        nx_d = dsm.sizes[xdimname]
        ny_d = dsm.sizes[ydimname]
        nz = dsm.sizes[zdimname]
        nz_stag = dsm.sizes[zdimname_stag]

        # 3D Variables
        height = (dsm['PH'] + dsm['PHB']) / 9.81
        pressure = dsm['P'] + dsm['PB']
        tk = dsm['T'] + dsm['T00']
        qv = dsm['QVAPOR']
        # rh = dsm['RH']
        umet = dsm['U']
        vmet = dsm['V']
        wa = dsm['W']
        # 2D variables
        XLAT = dsm['XLAT']
        XLONG = dsm['XLONG']
        T2 = dsm['T2']
        Q2 = dsm['Q2']
        PSFC = dsm['PSFC']
        U10 = dsm['U10']
        V10 = dsm['V10']
        # RAINNC = dsm['RAINNC']
        HGT = dsm['HGT']
        # import pdb; pdb.set_trace()

        # Remove attributes ('projection' in particular conflicts with Xarray)
        attrs_to_remove = ['FieldType', 'projection', 'MemoryOrder', 'stagger', 'coordinates', 'missing_value']
        for key in attrs_to_remove:
            height.attrs.pop(key, None)
            pressure.attrs.pop(key, None)
            tk.attrs.pop(key, None)
            qv.attrs.pop(key, None)
            # rh.attrs.pop(key, None)
            umet.attrs.pop(key, None)
            vmet.attrs.pop(key, None)
            wa.attrs.pop(key, None)
            # pwv.attrs.pop(key, None)
            T2.attrs.pop(key, None)
            Q2.attrs.pop(key, None)
            PSFC.attrs.pop(key, None)
            U10.attrs.pop(key, None)
            V10.attrs.pop(key, None)
            # PBLH.attrs.pop(key, None)
            # RAINNC.attrs.pop(key, None)
            HGT.attrs.pop(key, None)

        # Save variable attributes
        # pressure_attrs = pressure.attrs
        # height_attrs = height.attrs
        # temperature_attrs = tk.attrs
        pressure_attrs = {'description': 'Pressure', 'units': 'Pa'}
        height_attrs = {'description': 'Height', 'units': 'm'}
        temperature_attrs = {'description': 'Potential temperature', 'units': 'K'}
        qv_attrs = qv.attrs
        # rh_attrs = rh.attrs
        u_attrs = umet.attrs
        v_attrs = vmet.attrs
        w_attrs = wa.attrs
        T2_attrs = T2.attrs
        Q2_attrs = Q2.attrs
        PSFC_attrs = PSFC.attrs
        U10_attrs = U10.attrs
        V10_attrs = V10.attrs
        # RAINNC_attrs = RAINNC.attrs
        HGT_attrs = HGT.attrs

    else:
        # Create empty attributes
        pressure_attrs = ''
        height_attrs = ''
        temperature_attrs = ''
        qv_attrs = ''
        # rh_attrs = ''
        u_attrs = ''
        v_attrs = ''
        w_attrs = ''
        # PWV_attrs = ''
        T2_attrs = ''
        Q2_attrs = ''
        PSFC_attrs = ''
        U10_attrs = ''
        V10_attrs = ''
        # RAINNC_attrs = ''
        HGT_attrs = ''


    # Make array to store output
    # Number of tracks in the file
    out_ntracks = len(idx_track)
    out_nx = 2
    out_dims = (nz, out_nx)
    # Spatial coordinates
    xcoords = np.array([0, 1])
    zcoords = np.arange(0, nz)
    xcoords_attrs = {'long_name':'Location of points', 'comments':'0:fixed site, 1:CI location'}
    zcoords_attrs = {'long_name':'Vertical level'}
    out_coords = {
        'dims': out_dims,
        'xcoords': xcoords,
        'zcoords': zcoords,
        'xcoords_attrs': xcoords_attrs,
        'zcoords_attrs': zcoords_attrs,
        # 'DX': DX_sub,
        # 'DY': DY_sub,
    }
    # 3D variables
    out_P = np.full((out_ntracks, nz, out_nx), np.nan, dtype=float)
    out_T = np.full((out_ntracks, nz, out_nx), np.nan, dtype=float)
    out_QV = np.full((out_ntracks, nz, out_nx), np.nan, dtype=float)
    # out_RH = np.full((out_ntracks, nz, out_nx), np.nan, dtype=float)
    out_U = np.full((out_ntracks, nz, out_nx), np.nan, dtype=float)
    out_V = np.full((out_ntracks, nz, out_nx), np.nan, dtype=float)
    out_W = np.full((out_ntracks, nz, out_nx), np.nan, dtype=float)
    out_Z = np.full((out_ntracks, nz, out_nx), np.nan, dtype=float)

    # 2D variables
    out_T2 = np.full((out_ntracks, out_nx), np.nan, dtype=float)
    out_Q2 = np.full((out_ntracks, out_nx), np.nan, dtype=float)
    out_PSFC = np.full((out_ntracks, out_nx), np.nan, dtype=float)
    out_U10 = np.full((out_ntracks, out_nx), np.nan, dtype=float)
    out_V10 = np.full((out_ntracks, out_nx), np.nan, dtype=float)
    # out_PBLH = np.full((out_ntracks, out_nx), np.nan, dtype=float)
    out_HGT = np.full((out_ntracks, out_nx), np.nan, dtype=float)

    out_dict3d = None
    out_dict2d = None
    out_dict_attrs = None

    # Proceed if number of matched cell is > 0
    nmatchcell = len(idx_track)
    if (nmatchcell > 0):

        # Loop over each track
        for itrack in range(0, out_ntracks):
            # track center location (lat, lon)
            center = (_lat[itrack], _lon[itrack])
            # fixed location
            fixed_site = (lat_site, lon_site)                

            # wrfout file
            if wrfout_exist:
                # Find closest lat/lon index to fixed location
                fixlat_idx, fixlon_idx = location_to_idx(XLAT.data, XLONG.data, fixed_site)
                # Find closet lat/lon index to track center location
                lat_idx, lon_idx = location_to_idx(XLAT.data, XLONG.data, center)

                # Extract value at fixed site location
                if (fixlat_idx <= ny_d) & (fixlon_idx <= nx_d):
                    # 3D mass grid variables
                    out_T[itrack, :, 0] = tk.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    out_P[itrack, :, 0] = pressure.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    out_QV[itrack, :, 0] = qv.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    # out_RH[itrack, :, 0] = rh.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    out_U[itrack, :, 0] = umet.isel({ydimname:fixlat_idx, xdimname_stag:fixlon_idx}).data
                    out_V[itrack, :, 0] = vmet.isel({ydimname_stag:fixlat_idx, xdimname:fixlon_idx}).data
                    
                    # Interpolate staggered grid variables to mass grid
                    # Staggered grid has nz_stag levels, mass grid has nz levels
                    z_stag = height.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    w_stag = wa.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    # Linear interpolation: mass_level[k] = (stag[k] + stag[k+1]) / 2
                    out_Z[itrack, :, 0] = (z_stag[:-1] + z_stag[1:]) / 2.0
                    out_W[itrack, :, 0] = (w_stag[:-1] + w_stag[1:]) / 2.0

                    # 2D
                    out_T2[itrack, 0] = T2.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    out_Q2[itrack, 0] = Q2.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    out_PSFC[itrack, 0] = PSFC.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    out_U10[itrack, 0] = U10.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    out_V10[itrack, 0] = V10.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    # out_RAINNC[itrack, 0] = RAINNC.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data
                    out_HGT[itrack, 0] = HGT.isel({ydimname:fixlat_idx, xdimname:fixlon_idx}).data

                # Extract value at track center location
                if (lat_idx <= ny_d) & (lon_idx <= nx_d):
                    # 3D mass grid variables
                    out_T[itrack, :, 1] = tk.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    out_P[itrack, :, 1] = pressure.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    out_QV[itrack, :, 1] = qv.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    # out_RH[itrack, :, 1] = rh.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    out_U[itrack, :, 1] = umet.isel({ydimname:lat_idx, xdimname_stag:lon_idx}).data
                    out_V[itrack, :, 1] = vmet.isel({ydimname_stag:lat_idx, xdimname:lon_idx}).data
                    
                    # Interpolate staggered grid variables to mass grid
                    z_stag = height.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    w_stag = wa.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    # Linear interpolation: mass_level[k] = (stag[k] + stag[k+1]) / 2
                    out_Z[itrack, :, 1] = (z_stag[:-1] + z_stag[1:]) / 2.0
                    out_W[itrack, :, 1] = (w_stag[:-1] + w_stag[1:]) / 2.0

                    # 2D
                    out_T2[itrack, 1] = T2.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    out_Q2[itrack, 1] = Q2.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    out_PSFC[itrack, 1] = PSFC.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    out_U10[itrack, 1] = U10.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    out_V10[itrack, 1] = V10.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    # out_RAINNC[itrack, 1] = RAINNC.isel({ydimname:lat_idx, xdimname:lon_idx}).data
                    out_HGT[itrack, 1] = HGT.isel({ydimname:lat_idx, xdimname:lon_idx}).data


        # Put output variables to a dictionary for easier acceess
        out_dict3d = {
            'pressure': out_P,
            'height': out_Z,
            'temperature': out_T,
            'qv': out_QV,
            # 'rh': out_RH,
            'u': out_U,
            'v': out_V,
            'w': out_W,
        }
        out_dict2d = {
            # 'LCL': out_LCL,
            # 'LFC': out_LFC,
            # 'LPL': out_LPL,
            # 'LNB': out_LNB,
            # 'MUCAPE': out_MUCAPE,
            # 'MUCIN': out_MUCIN,
            # 'conv_core': out_convcore,
            # 'conv_mask': out_convmask,
            # 'tracknumber_cmask': out_tnmask,
            # 'tracknumber': out_tnmask,
            # 'comp_ref': out_refl,
            # 'dbz_comp': out_refl,
            # 'echotop10': out_eth10,
            # 'LWP': out_LWP,
            # 'IWP': out_IWP,
            # 'PWV': out_PWV,
            'T2': out_T2,
            'Q2': out_Q2,
            'PSFC': out_PSFC,
            'U10': out_U10,
            'V10': out_V10,
            # 'PBLH': out_PBLH,
            # 'RAINNC': out_RAINNC,
            'HGT': out_HGT,
        }
        out_dict_attrs = {
            'pressure': pressure_attrs,
            'height': height_attrs,
            'temperature': temperature_attrs,
            'qv': qv_attrs,
            # 'rh': rh_attrs,
            'u': u_attrs,
            'v': v_attrs,
            'w': w_attrs,
            # 'PWV': PWV_attrs,
            'T2': T2_attrs,
            'Q2': Q2_attrs,
            'PSFC': PSFC_attrs,
            'U10': U10_attrs,
            'V10': V10_attrs,
            # 'PBLH': PBLH_attrs,
            # 'RAINNC': RAINNC_attrs,
            'HGT': HGT_attrs,
            # 'DX': DX_sub,
            # 'DY': DY_sub,
        }
        # Merge attribute dictionaries
        # new_attrs = {**pixel_attrs, **met_attrs}
        # new_attrs = {**pixel_attrs, **cld_attrs}
        # out_dict_attrs = {**out_dict_attrs, **new_attrs}

    return out_dict3d, out_dict2d, out_dict_attrs, out_coords


#-----------------------------------------------------------------------
if __name__ == '__main__':

    # Get configuration file name from input
    config_file = sys.argv[1]

    # Read configuration from yaml file
    stream = open(config_file, 'r')
    config = yaml.full_load(stream)

    run_parallel = config['run_parallel']
    n_workers = config['n_workers']
    threads_per_worker = config['threads_per_worker']
    startdate = config['startdate']
    enddate = config['enddate']
    time_window = config['time_window']
    stats_path = config['stats_path']
    pixelfile_path = config['pixelfile_path']
    wrfout_path = config.get('wrfout_path', None)
    # metfile_path = config['metfile_path']
    # cldfile_path = config['cldfile_path']
    # metfile_path_2 = config['metfile_path_2']
    # cldfile_path_2 = config['cldfile_path_2']
    output_path = config['output_path']
    # met_filebase = config.get('met_filebase', None)
    # cld_filebase = config.get('cld_filebase', None)
    pixel_filebase = config['pixel_filebase']
    wrfout_filebase = config.get('wrfout_filebase')
    wrf_gridfile = config.get('wrf_gridfile')
    geolimits = config.get('geolimits')
    nhours = config['nhours']
    # ensmember = config['ensmember']
    # domain = config['domain']
    # nx = config['nx']
    # ny = config['ny']

    # Get subset domain limits
    lon_min, lon_max, lat_min, lat_max = geolimits

    # Read WRF grid file to get geolimits and grid spacing
    if wrf_gridfile is not None:
        print(f'Reading WRF grid file: {wrf_gridfile}')
        dsg = xr.open_dataset(wrf_gridfile).squeeze()
        # Get WRF lat/lon values
        lon_vals = dsg['XLONG'].values
        lat_vals = dsg['XLAT'].values
        dim_y = dsg.sizes['south_north']
        dim_x = dsg.sizes['west_east']
        dsg.close()

        # Find all points within the domain
        mask = ((lon_vals >= lon_min) & (lon_vals <= lon_max) & 
                (lat_vals >= lat_min) & (lat_vals <= lat_max))
        
        # Find bounding box in index space
        rows, cols = np.where(mask)

        if len(rows) > 0:
            # Get the min/max indices, with small buffer to capture edge points
            row_min = max(0, rows.min() - 1)
            row_max = min(lon_vals.shape[0], rows.max() + 2)
            col_min = max(0, cols.min() - 1)
            col_max = min(lon_vals.shape[1], cols.max() + 2)
        else:
            raise ValueError("No grid points found within the specified geolimits.")

        # Add bounding box indices to config for later use
        config['row_min'] = row_min
        config['row_max'] = row_max
        config['col_min'] = col_min
        config['col_max'] = col_max


    # Time threshold to match generated times with data times
    dt_thresh = 60.0     # [second]

    # Output statistics filename
    output_filename = f'{output_path}stats_env1d_2location_hourly_{startdate}_{enddate}.nc'
    os.makedirs(output_path, exist_ok=True)

    # Track statistics file dimension names
    tracks_dimname = 'tracks'
    times_dimname = 'times'
    z_dimname = 'z'
    y_dimname = 'y'
    x_dimname = 'x'

    # Track statistics file
    stats_filebase = 'trackstats_'
    trackstats_file = f'{stats_path}{stats_filebase}{startdate}_{enddate}.nc'

    # Read track statistics file
    print(trackstats_file)
    dsstats = xr.open_dataset(trackstats_file, decode_times=True)

    # # TODO: test subsetting a small number of tracks to speed up development
    # dsstats = dsstats.isel({tracks_dimname: slice(0, 10)})

    # Subset stats times to reduce array size
    ntracks = dsstats.sizes[tracks_dimname]
    ntimes = dsstats.sizes[times_dimname]
    coord_tracks = dsstats['tracks']
    stats_basetime = dsstats['base_time']
    stats_lon = dsstats['meanlon']
    stats_lat = dsstats['meanlat']
    time_res_hour = dsstats.attrs['time_resolution_hour']
    dsstats.close()

    print(f'Total Number of Tracks: {ntracks}')

    # Convert time resolution of data to minutes
    time_res_min = np.round(time_res_hour * 60).astype(int)
    ntimes_per_hour = np.round(60. / time_res_min).astype(int)

    # Select initiation time, and round to the nearest minute
    time0 = stats_basetime.isel(times=0).dt.round('min')
    # Get initiation lat/lon    
    stats_lon0 = stats_lon.isel(times=0).data
    stats_lat0 = stats_lat.isel(times=0).data

    # Make arrays to store matched hourly time for each track (single time point per track)
    # For CI at :00 min → match to prior hour
    # For CI at :15, :30, :45 min → match to same hour at :00
    matched_times = np.ndarray((ntracks,), dtype='datetime64[ns]')
    matched_basetimes = np.full((ntracks,), np.nan, dtype=float)
    matched_lons = np.full((ntracks,), np.nan, dtype=np.float32)
    matched_lats = np.full((ntracks,), np.nan, dtype=np.float32)

    # Get track data numpy arrays for better performance
    stats_min0 = time0.data
    # stats_mins = stats_basetime.dt.round(f'{time_res_min:.0f}min').data
    stats_mins = stats_basetime.data
    stats_lons = stats_lon.data
    stats_lats = stats_lat.data

    

    # Loop over each track to determine matched hourly time
    for itrack in range(0, ntracks):
        # Get CI time
        ci_time = pd.Timestamp(stats_min0[itrack])
        
        # Determine the matching hour for CI based on minutes
        if ci_time.minute == 0:
            # CI at exact hour (e.g., 06:00) → get previous hour (05:00)
            matched_hour = ci_time - pd.offsets.Hour(1)
        else:
            # CI at non-zero minutes (15, 30, 45) → get same hour at :00
            matched_hour = ci_time.floor('h')
        
        # Save matched time
        matched_times[itrack] = matched_hour
        matched_basetimes[itrack] = matched_hour.value / 1e9  # Convert to seconds since epoch
        
        # Use CI location (initiation lat/lon)
        matched_lons[itrack] = stats_lon0[itrack]
        matched_lats[itrack] = stats_lat0[itrack]


    # Attributes for matched times
    matched_basetimes_attrs = {
        'long_name': 'Matched hourly time for 3D environmental extraction',
        'units': 'Seconds since 1970-1-1',
        'comment': 'CI at :00 min → prior hour; CI at :15,:30,:45 min → same hour at :00',
    }

    # Find unique matched times (these are the files we need to read)
    uniq_times = np.unique(matched_times)
    uniq_times = uniq_times[~np.isnat(uniq_times)]
    nfiles = len(uniq_times)
    # Convert unique times to basetime
    uniq_basetimes = uniq_times.astype('datetime64[s]').astype(np.float64)

    print(f'Number of unique hourly times to extract: {nfiles}')

    # uniq_basetimes = np.array([tt.tolist()/1e9 for tt in uniq_times])



    ##############################################################
    # Call function to calculate statistics
    trackindices_all = []
    timeindices_all = []
    results = []

    if run_parallel == 1:
        # Initialize dask
        dask_tmp_dir = config.get("dask_tmp_dir", "/tmp")
        dask.config.set({'temporary-directory': dask_tmp_dir})
        cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker)
        client = Client(cluster)

    # Loop over each unique hourly time and call function to calculate
    for ifile in range(nfiles):
    # for ifile in range(0, 2):
        # Convert time string to match different input files
        itime = uniq_times[ifile]
        itime_wrfout = pd.to_datetime(str(itime)).strftime('%Y-%m-%d_%H:%M:%S')
        iyear_wrfout = pd.to_datetime(str(itime)).strftime('%Y')
        # itime_met = pd.to_datetime(str(itime)).strftime('%Y%m%d.%H%M%S')

        # File names (only wrfout file needed for hourly 3D data)
        if wrfout_path:
            fname_wrfout = f'{wrfout_path}{iyear_wrfout}/wrfout_d01_{itime_wrfout}'
        else:
            fname_wrfout = None
        # fname_met = fname_wrfout  # Use wrfout as met file
        # fname_cld = fname_wrfout  # Use wrfout as cld file
        # fname_pixel = None  # No pixel file needed for hourly data
        # import pdb; pdb.set_trace()

        # Find all tracks with matched time at this hour, within dt threshold
        idx_track = np.where(np.abs(matched_basetimes - uniq_basetimes[ifile]) < dt_thresh)[0]
        if len(idx_track) > 0:
            # Save match indices for the current file to the overall list
            trackindices_all.append(idx_track)
            timeindices_all.append(np.zeros_like(idx_track))  # Single time index (0) for each track

            # Get the track lat/lon values
            _lat = matched_lats[idx_track]
            _lon = matched_lons[idx_track]
            # import pdb; pdb.set_trace()

            # Serial
            if run_parallel == 0:
                result = extract_env_prof(
                    fname_wrfout, 
                    idx_track, 
                    _lat,
                    _lon,
                    config,
                )
                results.append(result)
            # Parallel
            elif run_parallel >= 1:
                result = dask.delayed(extract_env_prof)(
                    fname_wrfout, 
                    idx_track, 
                    _lat,
                    _lon,
                    config,
                )
                results.append(result)
            else:
                print(f'Invalid parallization option run_parallel: {run_parallel}')

    # import pdb; pdb.set_trace()
    
    final_results = results

    # Trigger dask computation
    if run_parallel == 0:
        final_results = results
    elif run_parallel >= 1:
        final_results = dask.compute(*results)
        wait(final_results)
    else:
        print(f'Invalid parallization option run_parallel: {run_parallel}')


    # Make a variable list and get attributes from one of the returned dictionaries
    # Loop over each return results till one that is not None
    counter = len(final_results)-1
    while counter > 0:
        if final_results[counter] is not None:
            var_names3d = list(final_results[counter][0].keys())
            var_names2d = list(final_results[counter][1].keys())
            var_attrs = final_results[counter][2]
            var_coords = final_results[counter][3]
            break
        counter -= 1
    
    # Loop over variable list to create the dictionary entry
    print(f'Creating output arrays ...')
    out_dict = {}
    out_dict_attrs = {}
    # Spatial coordinates
    out_nz, out_nx = var_coords['dims']
    xcoords = var_coords['xcoords']
    # ycoords = var_coords['ycoords']
    zcoords = var_coords['zcoords']
    xcoords_attrs = var_coords['xcoords_attrs']
    # ycoords_attrs = var_coords['ycoords_attrs']
    zcoords_attrs = var_coords['zcoords_attrs']

    var_names = var_names3d + var_names2d
    # 3D variables (ntracks, nz, nx) - no time dimension since single time per track
    for ivar in var_names3d:
        out_dict[ivar] = np.full((ntracks, out_nz, out_nx), np.nan, dtype=np.float32)
        out_dict_attrs[ivar] = var_attrs[ivar]
    # 2D variables (ntracks, nx)
    for ivar in var_names2d:
        out_dict[ivar] = np.full((ntracks, out_nx), np.nan, dtype=np.float32)
        out_dict_attrs[ivar] = var_attrs[ivar]

    # Put the results to output track stats variables
    # Loop over each file (parallel return results)
    # for ifile in range(nfiles):
    # Loop over each returned results
    for ifile in range(len(final_results)):
    # for ifile in range(0, 2):
        # Get the return results for this pixel file
        if final_results[ifile] is not None:
            iVAR3d = final_results[ifile][0]
            iVAR2d = final_results[ifile][1]
            if iVAR3d is not None:
                trackindices = trackindices_all[ifile]
                # Loop over each variable and assign values to output dictionary
                for ivar in var_names3d:
                    if iVAR3d[ivar].ndim == 3:
                        out_dict[ivar][trackindices,:,:] = iVAR3d[ivar]
            if iVAR2d is not None:
                trackindices = trackindices_all[ifile]
                # Loop over each variable and assign values to output dictionary
                for ivar in var_names2d:
                    if iVAR2d[ivar].ndim == 2:
                        out_dict[ivar][trackindices,:] = iVAR2d[ivar]
    # import pdb; pdb.set_trace()

    # Define a dataset containing all PF variables
    var_dict = {}
    print(f'Saving data to output arrays ...')
    # Define output variable dictionary
    for key, value in out_dict.items():
        if value.ndim == 2:
            var_dict[key] = (
                [tracks_dimname, x_dimname], value, out_dict_attrs[key],
            )
        if value.ndim == 3:
            var_dict[key] = (
                [tracks_dimname, z_dimname, x_dimname], value, out_dict_attrs[key],
            )
    # Add matched_basetime to output dictionary
    var_dict['matched_basetime'] = ([tracks_dimname], matched_basetimes, matched_basetimes_attrs)

    # Define coordinate list
    coord_dict = {
        tracks_dimname: ([tracks_dimname], coord_tracks.data, coord_tracks.attrs),
        z_dimname: ([z_dimname], zcoords, zcoords_attrs),
        # y_dimname: ([y_dimname], ycoords, ycoords_attrs),
        x_dimname: ([x_dimname], xcoords, xcoords_attrs),
    }
    gattr_dict = {
        'Title': 'Extracted hourly environment profiles at 2 locations for cell tracks',
        'Institution': 'Pacific Northwest National Laboratory',
        'Contact': 'zhe.feng@pnnl.gov',
        'Created_on': time.ctime(time.time()),
        'Note': 'Single matched hour per track: CI at :00 → prior hour; CI at :15,:30,:45 → same hour at :00',
    }

    # Define xarray dataset
    dsout = xr.Dataset(var_dict, coords=coord_dict, attrs=gattr_dict)

    # Set encoding/compression for all variables
    comp = dict(zlib=False, dtype='float32')
    encoding = {var: comp for var in dsout.data_vars}

    # Delete file if it already exists
    if os.path.isfile(output_filename):
        os.remove(output_filename)

    # Write to netcdf file
    print(f'Writing netCDF ...')
    dsout.to_netcdf(path=output_filename, mode='w', format='NETCDF4', 
                    unlimited_dims=tracks_dimname, encoding=encoding)
    print(f'Output saved as: {output_filename}')

    # import pdb; pdb.set_trace()