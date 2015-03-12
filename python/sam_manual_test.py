

from sam.languages.python import sscapi

ssc = sscapi.PySSC()
dat = ssc.data_create()

#ssc.data_set_number(data_object, variable_name, value)
#ssc.data_set_string(data_object, variable_name, value)
#ssc.data_set_array(data_object, variable_name, value)
#ssc.data_set_matrix(data_object, variable_name, value)

# -------------------------------------------
# add all of the variables in here (use the four methods described above)
ssc.data_set_number(dat, 'analysis_years', 1)
# -------------------------------------------


# create the compute module
utilityrate = ssc.module_create('utilityrate3')

# run the compute module using dat as input
ssc.module_exec(utilityrate, dat)

# get the results
ssc.data_get_array(dat, 'elec_cost_with_system_year1')
ssc.data_get_array(dat, 'elec_cost_without_system_year1')

# free the module
ssc.module_free(utilityrate)

# free the data
ssc.data_free(dat)