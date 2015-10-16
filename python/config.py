# -*- coding: utf-8 -*-
"""
Created on Wed Mar 26 13:01:56 2014

@author: mgleason
"""
#==============================================================================
# these are all variables that we can change, but don't want to expose to non-expert users
#==============================================================================


import os
import multiprocessing
import pandas as pd
import utility_functions as utilfunc

#==============================================================================
#  Show timing statements for functions
#==============================================================================
show_times = True

#==============================================================================
#   get postgres connection parameters
#==============================================================================
# get the path of the current file
path = os.path.dirname(os.path.abspath(__file__))

# set the name of the pg_params_file
#pg_params_file = 'pg_params.json'
pg_params_file = 'pg_params_bigde.json'
#pg_params_file = 'pg_params_dev.json'

# load pg params from pg_params.json
pg_params, pg_conn_string = utilfunc.get_pg_params(os.path.join(path, pg_params_file))

#==============================================================================
#   set the number of customer bins to model in each county
#==============================================================================
customer_bins = 10

#==============================================================================
#   model start year
#==============================================================================
start_year = 2014

#==============================================================================
#   Path to R will vary by user (until we move the script over to run on gispgdb server)
#==============================================================================
Rscript_paths = ['/usr/bin/Rscript','C:/Users/mgleason/Documents/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.3/bin/Rscript.exe', 'C:/Users/bsigrin/Documents/R/R-3.1.1/bin/Rscript.exe']
Rscript_path = None
for rp in Rscript_paths:   
    if os.path.exists(rp):
        Rscript_path = rp
if Rscript_path == None:
    raise Exception('No Rscript Path found: Add a new path to Rscripts_path in config.py')

#==============================================================================
#   set number of parallel processes to run postgres queries (this is ignored if parallelize = F)
#==============================================================================
npar = 8

#==============================================================================
#   local cores
#==============================================================================
local_cores = multiprocessing.cpu_count()/2

#==============================================================================
#  This has something to do with reeds mode
#==============================================================================
init_model = True


#==============================================================================
# Toggle gross fit vs net fit
#==============================================================================
# In non-NEM mode, should generation be allowed to offset self-consumption (False), 
# or directly sold to grid (True)
gross_fit_mode = False

#==============================================================================
#  Set a cap on value of incentives
#==============================================================================
# Maximum fraction of system capital cost that incentives offset 
# (effectively capping incentive value beyond this point)
max_incentive_fraction = 0.4

#==============================================================================
#  Set method for determining Bass parameters
#==============================================================================
# 'sunshot' - Use p,q parameters based on 2012 SunShot study; p = 0.0015, q = [0.3 - 0.5] increasing with metric_value 
# 'calibrated' - Use state and sector-specific p,q parameters based on fitting historic deployment to a Bass curve. 
bass_method = 'calibrated'
#bass_method = 'sunshot'

#==============================================================================
#  Set up the alpha lookup table used by datfunc.select_financing_and_tech
#==============================================================================
alpha_lkup = pd.DataFrame({'tech' : ['solar', 'solar', 'wind', 'wind'],
                           'business_model' : ['host_owned', 'tpo', 'host_owned', 'tpo'],
                            'alpha' : [2, 2, 2, 2]

})


#==============================================================================
#  Should the output schema be deleted after the model run
#==============================================================================
delete_output_schema = False

#==============================================================================
#  Should the model be run in calibration mode? [TEMPORARY CHANGES--delete!]
#==============================================================================
calibrate_mode = True
#calibrate_mode = False


#==============================================================================
#  Parameters for the calibrate_diffusion_params.py script
#==============================================================================
# pre-existing schema to use
static_schema = 'diffusion_results_2015_10_14_21h48m22s'

# list of p_scalars to test
p_scalar_list = [1, 10, 50, 100, 250, 500, 750, 1000]

# list of teq_yr1 values to test
teq_yr1_list = [2, 3, 3.5, 4]

# should scenario HTML reports be created?
# WARNING: Setting to True will slow down your runs dramatically
make_reports = False

# should full output results be saved to outputs.csv.gz?
# WARNING: Setting to True will slow down your runs dramatically
save_all_outputs = False
