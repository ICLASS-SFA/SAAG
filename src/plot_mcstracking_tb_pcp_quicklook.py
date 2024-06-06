import numpy as np
import glob, os, sys
import xarray as xr
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
import colormath, colormath.color_objects, colormath.color_conversions
from colormath.color_objects import sRGBColor
import urllib
import re
import copy
# For non-gui back end
mpl.use('agg')
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import dask
from dask.distributed import Client, LocalCluster
import warnings
warnings.filterwarnings("ignore")

#-----------------------------------------------------------------------
def truncate_colormap(cmap, minval=0.0, maxval=1.0, n=256):
    """ Truncate colormap.
    """
    new_cmap = mpl.colors.LinearSegmentedColormap.from_list(
        'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=minval, b=maxval),
        cmap(np.linspace(minval, maxval, n)))
    return new_cmap

#-----------------------------------------------------------------------
def plot_map_2panels(dataarray, lon, lat, ter, lon_ter, lat_ter, levels, cmaps, titles, cblabels, cbticks, timestr, figname):
    
    mpl.rcParams['font.size'] = 12
    mpl.rcParams['font.family'] = 'Helvetica'

    map_extend = [-82, -34, -56, 13]
    lonv = np.arange(-80,-30.1,10)
    latv = np.arange(-50,10.1,10)
    proj = ccrs.PlateCarree()
    levelshgt = [1000,6000]
    resolution = '50m'
    land = cfeature.NaturalEarthFeature('physical', 'land', resolution)
    ocean = cfeature.NaturalEarthFeature('physical', 'ocean', resolution)
    borders = cfeature.NaturalEarthFeature('cultural', 'admin_0_boundary_lines_land', resolution)

    fig = plt.figure(figsize=[10,8], dpi=100)
    gs = gridspec.GridSpec(2,2, height_ratios=[1,0.02], width_ratios=[1,1])
    gs.update(left=0.05, right=0.95, bottom=0.1, top=0.9, wspace=0.05, hspace=0.1)
    
    fig.text(0.5, 0.94, timestr, fontsize=14, ha='center')

    # Tb Panel
    ax1 = plt.subplot(gs[0,0], projection=proj)
    ax1.set_extent(map_extend, crs=proj)
    ax1.add_feature(borders, edgecolor='k', facecolor='none', linewidth=0.8, zorder=3)
    ax1.add_feature(land, facecolor='none', edgecolor='k', zorder=3)
    ax1.set_aspect('auto', adjustable=None)
    ax1.set_title(titles[0], loc='left')
    gl = ax1.gridlines(crs=proj, draw_labels=False, linestyle='--', linewidth=0.5)
    gl.xlocator = mpl.ticker.FixedLocator(lonv)
    gl.ylocator = mpl.ticker.FixedLocator(latv)        
    ax1.set_xticks(lonv, crs=ccrs.PlateCarree())
    ax1.set_yticks(latv, crs=ccrs.PlateCarree())
    lon_formatter = LongitudeFormatter(zero_direction_label=True)
    lat_formatter = LatitudeFormatter()        
    ax1.xaxis.set_major_formatter(lon_formatter)
    ax1.yaxis.set_major_formatter(lat_formatter)

    cmap = plt.get_cmap(cmaps[0])
    norm = mpl.colors.BoundaryNorm(levels[0], ncolors=cmap.N, clip=True)
    data = dataarray[0]
    Zm = np.ma.masked_where((np.isnan(data)), data)
    cf1 = ax1.pcolormesh(lon, lat, Zm, norm=norm, cmap=cmap, transform=ccrs.PlateCarree(), zorder=2)
    # Overplot cloudtracknumber boundary
    tn = np.copy(dataarray[2].data)
    # Replace all valid cloudtracknumber with a constant, and invalid cloudtracknumber with 0
    tn[(tn >= 1)] = 10
    tn[np.isnan(tn)] = 0
    # Overlay boundary of cloudtracknumber on Tb
    tn1 = ax1.contour(lon, lat, tn, colors='magenta', linewidths=1, alpha=0.5, transform=ccrs.PlateCarree(), zorder=5)
    # Tb Colorbar
    cax1 = plt.subplot(gs[1,0])
    cb1 = plt.colorbar(cf1, cax=cax1, label=cblabels[0], ticks=cbticks[0], extend='both', orientation='horizontal')
    # Terrain height
    ct = ax1.contour(lon_ter, lat_ter, ter, levels=levelshgt, colors='dimgray', linewidths=1, transform=proj, zorder=3)
    
    # Precipitation Panel
    ax2 = plt.subplot(gs[0,1], projection=ccrs.PlateCarree())
    ax2.set_extent(map_extend, crs=ccrs.PlateCarree())
    ax2.add_feature(borders, edgecolor='k', facecolor='none', linewidth=0.8, zorder=3)
    ax2.add_feature(land, facecolor='none', edgecolor='k', zorder=3)
    ax2.set_aspect('auto', adjustable=None)
    ax2.set_title(titles[1], loc='left')
    gl = ax2.gridlines(crs=proj, draw_labels=False, linestyle='--', linewidth=0.5)
    gl.xlocator = mpl.ticker.FixedLocator(lonv)
    gl.ylocator = mpl.ticker.FixedLocator(latv)        
    ax2.set_xticks(lonv, crs=ccrs.PlateCarree())
    # ax2.set_yticks(latv, crs=ccrs.PlateCarree())
    lon_formatter = LongitudeFormatter(zero_direction_label=True)
    lat_formatter = LatitudeFormatter()        
    ax2.xaxis.set_major_formatter(lon_formatter)
    # ax2.yaxis.set_major_formatter(lat_formatter)

    # MCS track number mask
    cmap = plt.get_cmap(cmaps[2])
    data = dataarray[2]
    norm = mpl.colors.BoundaryNorm(levels[2], ncolors=cmap.N, clip=True)
    Zm = np.ma.masked_invalid(data)
    cm1 = ax2.pcolormesh(lon, lat, Zm, norm=norm, cmap=cmap, transform=ccrs.PlateCarree(), zorder=2, alpha=0.7)
    
    # Precipitation
    cmap = plt.get_cmap(cmaps[1])
    norm = mpl.colors.BoundaryNorm(levels[1], ncolors=cmap.N, clip=True)
    data = dataarray[1]
    Zm = np.ma.masked_where(((data < 2)), data)
    cf2 = ax2.pcolormesh(lon, lat, Zm, norm=norm, cmap=cmap, transform=ccrs.PlateCarree(), zorder=2)
    # Colorbar
    cax2 = plt.subplot(gs[1,1])
    cb2 = plt.colorbar(cf2, cax=cax2, label=cblabels[1], ticks=cbticks[1], extend='both', orientation='horizontal')
    # Terrain height
    ct = ax2.contour(lon_ter, lat_ter, ter, levels=levelshgt, colors='dimgray', linewidths=1, transform=proj, zorder=3)
    
    #     if oob_colors is not None:
#     oob_colors = {'under':'white', 'over':'magenta'}
#     cf2.cmap.set_over(oob_colors['over'])
#     cf2.cmap.set_under(oob_colors['under'])
#     cf2.set_clim(min(levels[1]), max(levels[1]))

    # fig.savefig(figname, dpi=300, bbox_inches='tight', facecolor='w')
    # Thread-safe figure output
    canvas = FigureCanvas(fig)
    canvas.print_png(figname)
    fig.savefig(figname, facecolor='w')
    return fig

#-----------------------------------------------------------------------
def work_for_time_loop(ds, tnlev, topfile, figdir):
    # Read topography data
    dstop = xr.open_dataset(topfile)
    # Convert surface geopotential to height in meters
    ter = dstop.HGT.squeeze()
    # landmask = dstop.LANDMASK.squeeze()
    # ter_s = gaussian_filter(ter, 1)
    lon_ter = dstop.lon
    lat_ter = dstop.lat

    pcplev = [2,3,4,5,6,8,10,15]
    # tnlev = tn_new
    levels = [np.arange(200, 320.1, 10), pcplev, tnlev]
    cbticks = [np.arange(200, 320.1, 20), pcplev]
    cblabels = ['Tb (K)', 'Precipitation (mm h$^{-1}$)']
    cmap_tb = truncate_colormap(plt.get_cmap('jet'), minval=0.05, maxval=0.95)
    cmap_pcp = truncate_colormap(plt.get_cmap('viridis'), minval=0.2, maxval=1.0)
    cmap_mcs = truncate_colormap(plt.get_cmap('jet_r'), minval=0.05, maxval=0.95)
    cmaps = [cmap_tb, cmap_pcp, cmap_mcs]
    titles = ['(a) IR Brightness Temperature','(b) Precipitation (Tracked MCSs Shaded)']

    dataarr = [ds.tb, ds.precipitation, ds.cloudtracknumber]
    fdatetime = pd.to_datetime(ds.time.values).strftime('%Y%m%d_%H%M')
    timestr = pd.to_datetime(ds.time.values).strftime('%Y-%m-%d %H:%M UTC')
    figname = f'{figdir}{fdatetime}.png'
    # print(timestr)
    fig = plot_map_2panels(dataarr, lon, lat, ter, lon_ter, lat_ter, 
                            levels, cmaps, titles, cblabels, cbticks, timestr, figname)
    plt.close(fig)
    print(timestr)

    # import pdb; pdb.set_trace()
    return 1


if __name__ == "__main__":
    start_datetime = sys.argv[1]
    end_datetime = sys.argv[2]
    run_parallel = int(sys.argv[3])

    topfile = '/global/project/projectdirs/m1657/zfeng/SAAG/map_data/wrf_landmask_reg2imerg.nc'

    # datadir = f'/global/cscratch1/sd/feng045/SAAG/June2018_May2019_nsp/mcstracking/20180601_20190630/'
    # datadir = f'/global/cscratch1/sd/feng045/SAAG/GPM/mcstracking/20180601_20190630/'
    # datadir = f'/global/cscratch1/sd/feng045/SAAG/GPM/mcstracking_4pyflex/20190122_20190126/'
    datadir = f'/global/cscratch1/sd/feng045/SAAG/mcs_tracking/3year_test_simulation/GPM/mcstracking/20180601.0000_20190601.0000/'
    # datadir = f'/global/cscratch1/sd/feng045/SAAG/mcs_tracking/3year_test_simulation/WRF/mcstracking/20180601.0000_20190601.0000/'

    # Generate time marks within the start/end datetime
    mcs_datetimes = pd.date_range(start=start_datetime, end=end_datetime, freq='1H').strftime('%Y%m%d_%H')

    datafiles = []
    pwfiles = []
    for tt in range(0, len(mcs_datetimes)):
        datafiles.extend(sorted(glob.glob(f'{datadir}mcstrack_{mcs_datetimes[tt]}*.nc')))
    print(f'Number of files: {len(datafiles)}')
        
    date_range = f'{os.path.basename(datafiles[0])[9:17]}_{os.path.basename(datafiles[-1])[9:17]}'
    figdir = f'{datadir}quicklooks/{date_range}/'
    os.makedirs(f'{figdir}', exist_ok=True)
    print(figdir)

    # Read data
    ds = xr.open_mfdataset(datafiles, concat_dim='time', combine='nested', 
            drop_variables=['longitude','latitude','cloudnumber','pcptracknumber','numclouds'])
    # ds.load()
    lon = ds.lon
    lat = ds.lat
    ntimes = ds.dims['time']
    tnlev = np.arange(ds.cloudtracknumber.min(), ds.cloudtracknumber.max(), 1)

    # Serial option
    if run_parallel == 0:
        for itime in range(0, ntimes):
            ids = ds.isel(time=itime)
            result = work_for_time_loop(ids, tnlev, topfile, figdir)
            # import pdb; pdb.set_trace()

    # Parallel option
    elif run_parallel == 1:

        # Set up dask workers and threads
        n_workers = 32

        # Initialize dask
        cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
        client = Client(cluster)

        results = []
        for itime in range(0, ntimes):
            ids = ds.isel(time=itime)
            result = dask.delayed(work_for_time_loop)(ids, tnlev, topfile, figdir)
            results.append(result)

        # Trigger dask computation
        final_result = dask.compute(*results)