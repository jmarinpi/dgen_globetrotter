# -*- coding: utf-8 -*-
"""
Created on Wed Mar 26 13:01:56 2014

@author: mgleason
"""

import os

#==============================================================================
# these are all variables that we can change, but don't want to expose to non-expert users
#==============================================================================

pg_params = {'host'     : 'gispgdb',
             'dbname'   : 'dav-gis',
             'user'     : 'mgleason',
             'password' : 'mgleason'
                }

pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s' % pg_params

# input spreadsheet location (relative to python folder)
input_xls = '../excel/DG_wind_01_16_2014_named_ranges.xlsm'

# number of customer bins to model in each county
customer_bins = 10

# seed for random sampling of customers from each county
# (any integer value is allowed)
random_generator_seed = 1

# allowable flex factor for oversizing or undersizing turbines (used in selection of optimal turbine height and size for each customer bin)
oversize_turbine_factor = 1.15
undersize_turbine_factor = 0.5

# model start year
start_year = 2014

preprocess = False #True means to use the last-created main table

# Path to R will vary by user (until we move the script over to run on gispgdb server)
Rscript_paths = ['/usr/bin/Rscript','C:/Users/mgleason/Documents/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.2/bin/Rscript.exe' ]
Rscript_path = None
for rp in Rscript_paths:   
    if os.path.exists(rp):
        Rscript_path = rp
if Rscript_path == None:
    raise Exception('No Rscript Path found: Add a new path to Rscripts_path in config.py')

# set boolean variable for parallelization
parallelize = True
# set number of parallel processes to run (this is ignored if parallelize = F)
npar = 4

# load scenario input sheet?
load_scenario_inputs = True

# Should a html report file be created?
create_report = True
init_model = True

