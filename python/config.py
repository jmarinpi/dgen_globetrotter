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
import json
import agent_preparation_geo
import agent_preparation_elec
import agent_mutation_geo
import agent_mutation_elec
import financial_functions_geo
import financial_functions_elec
import diffusion_functions_geo
import diffusion_functions_elec
import supply_curve_geo
import supply_curve_elec


#==============================================================================
#   get postgres connection parameters
#==============================================================================
# get the path of the current file
path = os.path.dirname(os.path.abspath(__file__))

# set the name of the pg_params_file
#pg_params_file = 'pg_params.json'
pg_params_file = 'pg_params_gis.json'
#pg_params_file = 'pg_params_bigde.json'

# load pg params from pg_params.json
pg_params, pg_conn_string = utilfunc.get_pg_params(os.path.join(path, pg_params_file))
pg_params_log = json.dumps(json.loads(pd.DataFrame([pg_params])[['host', 'port', 'dbname', 'user']].ix[0].to_json()), indent = 4, sort_keys = True)
#==============================================================================
#   set the number of customer bins to model in each county
#==============================================================================
agents_per_region = 5
sample_pct = 0.02
min_agents = 3

#==============================================================================
#   model start year
#==============================================================================
start_year = 2014

#==============================================================================
#   module lookup based on tech mode
#==============================================================================
module_lkup = {
                'agent_preparation' : 
                    { 
                        'geo' : agent_preparation_geo,
                        'elec': agent_preparation_elec,
                    },
                'agent_mutation' :
                    { 
                        'geo' : agent_mutation_geo,
                        'elec': agent_mutation_elec,
                    },
                'financial_functions' :
                    { 
                        'geo' : financial_functions_geo,
                        'elec': financial_functions_elec,
                    },
                'diffusion_functions' :                                
                    { 
                        'geo' : diffusion_functions_geo,
                        'elec': diffusion_functions_elec,
                    },
                'supply_curve'  :
                    {
                        'geo' : supply_curve_geo,
                        'elec': supply_curve_elec
                    }
}


#==============================================================================
#   Path to R will vary by user (until we move the script over to run on gispgdb server)
#==============================================================================
Rscript_paths = ['/usr/bin/Rscript','C:/Users/mgleason/Documents/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.3/bin/Rscript.exe', 'C:/Users/bsigrin/Documents/R/R-3.1.1/bin/Rscript.exe', 'C:/Users/pgagnon/Documents/R/R-3.2.2/bin/Rscript.exe']
Rscript_path = None
for rp in Rscript_paths:   
    if os.path.exists(rp):
        Rscript_path = rp
if Rscript_path == None:
    raise Exception('No Rscript Path found: Add a new path to Rscripts_path in config.py')

#==============================================================================
#   set number of parallel processes to run postgres queries (this is ignored if parallelize = F)
#==============================================================================
pg_procs = 8

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
#  Set method for determining Bass parameters
#==============================================================================
# 'sunshot' - Use p,q parameters based on 2012 SunShot study; p = 0.0015, q = [0.3 - 0.5] increasing with metric_value 
# 'user_input' - Use state and sector-specific p,q parameters based on input sheet. Solar defaults in input sheet are based on fitting historic deployment to a Bass curve. 
bass_method = 'user_input'
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
delete_output_schema = True

#==============================================================================
#  Do you want to use an existing schema?

#  Warning: Using an existing schema will skip the following steps:
#   1) generation of agents, 
#   2) bill savings calculations
#   3) evaluation of agent tech potential against tech potential caps
# Because some scenario inputs are embedded in agents and bill savings calcs,
# using an existing schema for multiple scenarios may not yield correct results,
# depending on which scenario inputs you modify.
# Refer to https://github.nrel.gov/dg-wind/diffusion/blob/dev_misc/docs/existing_schema_inputs.csv
# to determine whether this is a safe setting  for your scenario analysis.
#==============================================================================
use_existing_schema = False
# change this to the schema with existing agents/bill savings that you want to use
existing_schema_name = 'diffusion_results_2016_01_29_11h31m03s'


#==============================================================================
#  Parameters for the calibrate_diffusion_params.py script
#==============================================================================
# pre-existing schema to use
static_schema = 'diffusion_results_2015_11_12_15h07m43s'

# list of p values to test
override_p_values = [0.0004, 0.0005]

# list of q values to test
override_q_values = [0.3, 0.4]

# list of teq_yr1 values to test
override_teq_yr1_values = [2, 3]

# should scenario HTML reports be created?
# WARNING: Setting to True will slow down your runs dramatically
make_reports = False

# should full output results be saved to outputs.csv.gz?
# WARNING: Setting to True will slow down your runs dramatically
save_all_outputs = False
