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
start_year = 2014

#==============================================================================
#   Path to R will vary by user (until we move the script over to run on gispgdb server)
#==============================================================================
Rscript_paths = ['/nopt/nrel/apps/R/3.2.2-gcc/bin/Rscript','/usr/bin/Rscript','/usr/local/bin/Rscript','C:/Program Files/R/R-3.0.2/bin/Rscript.exe','C:/Program Files/R/R-3.0.3/bin/Rscript.exe', 'C:/Users/bsigrin/Documents/R/R-3.1.1/bin/Rscript.exe', 'C:/Program Files/R/R-3.3.2/bin/Rscript.exe']

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
