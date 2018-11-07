# -*- coding: utf-8 -*-
"""
Edited Monday Nov 5, 218
@author: tkwasnik
"""


import os
import multiprocessing

#==============================================================================
# these are all variables that we can change, but don't want to expose to non-expert users
#==============================================================================

#==============================================================================
#   get postgres connection parameters
#==============================================================================
# get the path of the current file
model_path = os.path.dirname(os.path.abspath(__file__))

# set the name of the pg_params_file
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
start_year = 2016

#==============================================================================
#   Path to R will vary by user (until we move the script over to run on gispgdb server)
#==============================================================================
Rscript_paths = ['/usr/bin/Rscript','/usr/local/bin/Rscript']

#==============================================================================
#   set number of parallel processes to run postgres queries (this is ignored if parallelize = F)
#==============================================================================
pg_procs = 12

#==============================================================================
#   local cores
#==============================================================================
local_cores = multiprocessing.cpu_count()/2

#==============================================================================
#  Should the output schema be deleted after the model run
#==============================================================================
delete_output_schema = False
