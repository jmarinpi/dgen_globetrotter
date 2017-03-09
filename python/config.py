# -*- coding: utf-8 -*-
"""
Created on Wed Mar 26 13:01:56 2014

@author: mgleason
"""

import os
import multiprocessing
import pandas as pd
import sys

#==============================================================================
# these are all variables that we can change, but don't want to expose to non-expert users
#==============================================================================

#==============================================================================
#   mode
# options are: ['run', 'develop', 'setup_develop']
#==============================================================================
mode = 'run'

#==============================================================================
#   get postgres connection parameters
#==============================================================================
# get the path of the current file
model_path = os.path.dirname(os.path.abspath(__file__))

# set the name of the pg_params_file
#pg_params_file = 'pg_params.json'
#pg_params_file = 'pg_params_gis.json'
#pg_params_file = 'pg_params_bigde.json'
pg_params_file = 'pg_params_atlas.json'

#==============================================================================
#   set the number of customer bins to model in each county
#==============================================================================
agents_per_region = 1
sample_pct = 0.02
min_agents = 3

#==============================================================================
#   model start year
#==============================================================================
start_year = 2014

#==============================================================================
#   Path to R will vary by user (until we move the script over to run on gispgdb server)
#==============================================================================
Rscript_paths = ['/usr/bin/Rscript','C:/Users/mgleason/Documents/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.3/bin/Rscript.exe', 'C:/Users/bsigrin/Documents/R/R-3.1.1/bin/Rscript.exe', 'C:/Users/pgagnon/Documents/R/R-3.2.2/bin/Rscript.exe']

#==============================================================================
#   Path to support functions repo will vary by user
#==============================================================================
support_repo_paths = ['support_functions/python',
					  'C:/users/pgagnon/desktop/support_functions/python',
                      '/Users/mmooney/Documents/github/support_functions/python',
                      '/srv/data/home/pgagnon/support_functions/python']
for path in support_repo_paths:
    sys.path.append(path)

#==============================================================================
#   set number of parallel processes to run postgres queries (this is ignored if parallelize = F)
#==============================================================================
pg_procs = 12

#==============================================================================
#   local cores
#==============================================================================
local_cores = multiprocessing.cpu_count()/2

#==============================================================================
# tech_choice decision variable
#==============================================================================
tech_choice_decision_var = 'max_market_share'
# alternative options are: npv4 or npv


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
# TEMPORARY PATCH FOR STORAGE BRANCH
# TODO: delete after solar+storage is addded as an option to the excel input sheet
# Temporary input for solar+storage mode (if true, overrides techs in input sheet to run solar + storage)
solar_plus_storage_mode = True
#==============================================================================
