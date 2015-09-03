# -*- coding: utf-8 -*-
"""
Created on Wed Mar 26 13:01:56 2014

@author: mgleason
"""

import os
import json
import multiprocessing

#==============================================================================
# these are all variables that we can change, but don't want to expose to non-expert users
#==============================================================================

#pg_params_file = 'pg_params.json'
#pg_params_file = 'pg_params_db.json'
pg_params_file = 'pg_params_dev.json'

# load pg params from pg_params.json
path = os.path.dirname(os.path.abspath(__file__))
pg_params_json = file(os.path.join(path, pg_params_file),'r')
pg_params = json.load(pg_params_json)
pg_params_json.close()

pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s port=%(port)s' % pg_params

# input spreadsheet location (relative to python folder)
input_xls = '../excel/DG_wind_01_16_2014_named_ranges.xlsm'

# number of customer bins to model in each county
customer_bins = 10

# seed for random sampling of customers from each county
# (any integer value is allowed)
random_generator_seed = 1

# allowable flex factor for oversizing or undersizing turbines (used in selection of optimal turbine height and size for each customer bin)
oversize_system_factor = 1.15
undersize_system_factor = 0.5

# model start year
start_year = 2014

preprocess = False #True means to use the last-created main table

# Path to R will vary by user (until we move the script over to run on gispgdb server)
Rscript_paths = ['/usr/bin/Rscript','C:/Users/mgleason/Documents/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.3/bin/Rscript.exe', 'C:/Users/bsigrin/Documents/R/R-3.1.1/bin/Rscript.exe']
Rscript_path = None
for rp in Rscript_paths:   
    if os.path.exists(rp):
        Rscript_path = rp
if Rscript_path == None:
    raise Exception('No Rscript Path found: Add a new path to Rscripts_path in config.py')

# set boolean variable for parallelization
parallelize = True
# set number of parallel processes to run postgres queries (this is ignored if parallelize = F)
npar = 4

# local cores
local_cores = multiprocessing.cpu_count()/2

# load scenario input sheet?
load_scenario_inputs = True

# Should a html report file be created?
create_report = True
init_model = True

# In non-NEM mode, should generation be allowed to offset self-consumption (False), or directly sold to grid (True)
gross_fit_mode = False

# Maximum fraction of system capital cost that incentives offset (effectively capping incentive value beyond this point)
max_incentive_fraction = 0.4

# model mode (solar or wind)
#technology = 'wind'
technology = 'solar'


if technology == 'solar':
    tech_lifetime = 25
elif technology == 'wind':
    tech_lifetime = 25
