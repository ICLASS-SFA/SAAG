# SAAG Convective Cloud Analysis

This repo contains codes for convective cloud analysis using the [NCAR SAAG](https://ral.ucar.edu/projects/south-america-affinity-group-saag) simulations.

## Subset auxiliary files

SAAG has auxiliary files with 15 min output frequency, saved as daily files. To subset those files within a domain and period, run this:

`python subset_vars_from_aux.py config_subset_aux_goamazon.yml`

The codes and config files are in the /src directory. The code can be run in parallel using Dask, specified in the config file. 