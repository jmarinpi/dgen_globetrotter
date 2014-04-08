# -*- coding: utf-8 -*-
"""
Created on Wed Mar 26 13:01:56 2014

@author: mgleason
"""

#==============================================================================
# these are all variables that we can change, but don't want to expose to non-expert users
#==============================================================================

pg_params = {'host'     : 'gispgdb',
             'dbname'   : 'dav-gis',
             'user'     : 'bsigrin',
             'password' : 'bsigrin'
                }

pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s' % pg_params

# input spreadsheet location (relative to python folder)
input_xls = '..\excel\DG_wind_01_16_2014_named_ranges.xlsm'

# number of customer bins to model in each county
customer_bins = 10

# seed for random sampling of customers from each county
random_generator_seed = 1

# allowable flex factor for oversizing or undersizing turbines (used in selection of optimal turbine height and size for each customer bin)
oversize_turbine_factor = 1.15
undersize_turbine_factor = 0.5

# model start year
start_year = 2014

preprocess = True

# Path to R will vary by user (until we move the script over to run on gispgdb server)
    # Mike's path
Rscript_path = 'C:/Users/mgleason/Documents/R/R-3.0.2/bin/Rscript.exe'
    # Ben's Path
#Rscript_path = 'C:/Program Files/R/R-3.0.2/bin/Rscript.exe' 


