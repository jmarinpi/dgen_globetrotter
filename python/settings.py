# -*- coding: utf-8 -*-
"""
Created on Wed Sep 14 10:48:46 2016

@author: mgleason
"""
import os
import warnings
import json
import pandas as pd
import utility_functions as utilfunc
import multiprocessing
import sys

#%%
class ModelSettings(object):
    
    def __init__(self):
        
        self.model_init = None # type is float
        self.cdate = None # type is text
        self.out_dir = None #  doesn't exist already, check parent folder exists
        self.start_year = None # must = 2014
        self.Rscript_path = None # path exists
        self.input_scenarios = None # type is list, is not empty
        self.git_hash = None # type is text
        self.pg_params_file = None # path exists
        self.pg_params = None # type is dict, includes all elements
        self.pg_conn_string = None # type is text
        self.pg_params_log = None # type is text, doesn't include pw 
        self.model_path = None # path exists
        self.use_existing_schema = None # type is boolean
        self.existing_schema_name = None # type is text
        self.agents_per_region = None # type is integer, > 0
        self.sample_pct = None # type is float, <=1, warn if> 0.05 about slow run times
        self.min_agents = None # type is integer, >=0
        self.pg_procs = None # int<=16
        self.local_cores = None # int < cores on machine
        self.tech_choice_decision_var = None # one of ['max_market_share', 'npv4', 'npv']
        self.delete_output_schema = None # bool
        self.mode = None # one of ['run', 'develop', 'setup_develop']
        #==============================================================================
        # TEMPORARY PATCH FOR STORAGE BRANCH    
        # TODO: delete solar_plus_storage_mode after solar+storage is addded as optinon to the excel input sheet
        self.solar_plus_storage_mode = None # boolean (temporary)
        #==============================================================================

    def set(self, attr, value):

        self.__setattr__(attr, value)
        self.validate_property(attr)
        
        
    def get(self, attr):

        return self.__getattribute__(attr)

    
    def add_config(self, config):
        
        self.set('start_year', config.start_year)
        self.set('model_path', config.model_path)
        self.set('use_existing_schema', config.use_existing_schema)
        self.set('existing_schema_name', config.existing_schema_name)
        self.set('agents_per_region', config.agents_per_region)
        self.set('sample_pct', config.sample_pct)
        self.set('min_agents', config.min_agents)
        self.set('local_cores', config.local_cores)
        self.set('pg_procs', config.pg_procs)
        self.set_pg_params(config.pg_params_file)
        self.set('tech_choice_decision_var', config.tech_choice_decision_var)
        self.set('delete_output_schema', config.delete_output_schema)
        self.set('mode', config.mode)
        #==============================================================================
        # TEMPORARY PATCH FOR STORAGE BRANCH    
        self.set('solar_plus_storage_mode', config.solar_plus_storage_mode) # TODO: delete after solar + storage is addded to the excel input sheet
        #==============================================================================


    def set_pg_params(self, pg_params_file):
        
        # check that it exists
        pg_params, pg_conn_string = utilfunc.get_pg_params(os.path.join(self.model_path, pg_params_file))
        pg_params_log = json.dumps(json.loads(pd.DataFrame([pg_params])[['host', 'port', 'dbname', 'user']].ix[0].to_json()), indent = 4, sort_keys = True)

        self.set('pg_params_file', pg_params_file)
        self.set('pg_params', pg_params)
        self.set('pg_conn_string', pg_conn_string)
        self.set('pg_params_log', pg_params_log)
        
        
    def set_Rscript_path(self, Rscript_paths):
        
        for rp in Rscript_paths:   
            if os.path.exists(rp):
                    self.Rscript_path = rp
        if self.Rscript_path == None:
            raise ValueError('No Rscript Path found: Add a new path to Rscripts_path in config.py')

    def validate_property(self, property_name):
        
        # for all properties -- check not null
        if self.get(property_name) == None:
            raise ValueError('%s has not been set' % property_name)

        # validation for specific properties
        if property_name == 'model_init':
            # check type
            try:
                check_type(self.get(property_name), float)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))


        elif property_name == 'cdate':
            # check type
            try:
                check_type(self.get(property_name), str)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            
            
        elif property_name == 'out_dir':
            # check type
            try:
                check_type(self.get(property_name), str)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))                   

            
        elif property_name == 'start_year':
            # check type
            try:
                check_type(self.get(property_name), int)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))                       
            # assert equals 2014
            if self.start_year <> 2014:
                raise ValueError('Invalid %s: must be set to 2014' % property_name)


        elif property_name == 'Rscript_path':
            # check type
            try:
                check_type(self.get(property_name), str)     
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))                   
            # path exists
            if os.path.exists(self.Rscript_path) == False:
                raise ValueError('Invalid %s: does not exist' % property_name)


        elif property_name == 'input_scenarios':
            # check type
            try:
                check_type(self.get(property_name), list)              
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))                   
            if len(self.input_scenarios) == 0:
                raise ValueError("Invalid %s: No input scenario spreadsheet were found in the input_scenarios folder." % property_name)      


        elif property_name == 'git_hash':
            # check type
            try:        
                check_type(self.get(property_name), str)              
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))                   


        elif property_name == 'pg_params_file':
            # check type
            try:
                check_type(self.get(property_name), str)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))                   
            # check the path exists
            if os.path.exists(self.pg_params_file) == False:
                raise ValueError('Invalid %s: does not exist' % property_name)


        elif property_name == 'pg_params':
            # check type
            try:
                check_type(self.get(property_name), dict) 
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            # check for all values
            required_keys = set(['dbname',
                             'host',
                             'port',
                             'password',
                             'user'])
            if set(self.pg_params.keys()).issubset(required_keys) == False:
                raise ValueError('Invalid %s: missing required keys (%s)' % (property_name, required_keys))
            

        elif property_name == 'pg_conn_string':
            # check type
            try:
                check_type(self.get(property_name), unicode) 
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))            


        elif property_name == 'pg_params_log':
            # check type
            try:
                check_type(self.get(property_name), str)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            # check password is not included
            if 'password' in self.pg_params_log:
                raise ValueError('Invalid %s: password shoud not be included' % property_name)


        elif property_name == 'model_path':
            # check type
            try:
                check_type(self.get(property_name), str)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            # check the path exists
            if os.path.exists(self.model_path) == False:
                raise ValueError('Invalid %s: does not exist' % property_name)


        elif property_name == 'use_existing_schema':
            # check type
            try:
                check_type(self.get(property_name), bool)     
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       


        elif property_name == 'existing_schema_name':
            # check type
            try:
                check_type(self.get(property_name), str)     
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       

        elif property_name == 'agents_per_region':
            # check type
            try:
                check_type(self.get(property_name), int)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            if self.agents_per_region <= 0:
                raise ValueError('Invalid %s: value must be >0' % property_name)
            if self.agents_per_region > 20:
                warnings.warn('High %s: using values > 20 may result in very slow model run times' % property_name)

        elif property_name == 'sample_pct':
            # check type
            try:
                check_type(self.get(property_name), float)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            if self.sample_pct > 1:
                raise ValueError('Invalid %s: value must be <= 0' % property_name)
            if self.sample_pct > 0.05:
                warnings.warn('High %s: using values > 0.05 may result in very slow model run times' % property_name)
    
            
        elif property_name == 'min_agents':
            # check type
            try:
                check_type(self.get(property_name), int)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            # >= 0
            if self.min_agents < 0:
                raise ValueError('Invalid %s: value must be >= 0' % property_name)


        elif property_name == 'local_cores':
            # check type
            try:
                check_type(self.get(property_name), int)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            # >= 0
            if self.min_agents < 0:
                raise ValueError('Invalid %s: value must be >= 0' % property_name)
            # check if too large
            if self.local_cores > multiprocessing.cpu_count():
                raise ValueError('Invalid %s: value exceeds number of CPUs on local machine' % property_name)

        elif property_name == 'pg_procs':
            # check type
            try:
                check_type(self.get(property_name), int)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))       
            # >= 0
            if self.min_agents < 0:
                raise ValueError('Invalid %s: value must be >= 0' % property_name)
            # warn if too large
            if self.pg_procs > 16:
                warnings.warn("High %s: may saturate the resources of the Postgres server" % property_name)
                
        elif property_name == 'tech_choice_decision_var':
            # check type
            try:
                check_type(self.get(property_name), str)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))      
                
            # one of ['max_market_share', 'npv4', 'npv']
            valid_opts = ['max_market_share', 'npv4', 'npv']
            if self.tech_choice_decision_var not in valid_opts:
                raise ValueError('Invalid %s: must be one of %s' % (property_name, valid_opts))
                
        elif property_name == 'delete_output_schema':
            # check type
            try:
                check_type(self.get(property_name), bool)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))           
        
        elif property_name == 'mode':
            # check type
            try:
                check_type(self.get(property_name), str)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))   
            
            # one of ['run', 'develop', 'setup_develop']
            valid_opts = ['run', 'develop', 'setup_develop']
            if self.mode not in valid_opts:
                raise ValueError('Invalid %s: must be one of %s' % (property_name, valid_opts))                

        #==============================================================================
        # TEMPORARY PATCH FOR STORAGE BRANCH        
        # TODO: delete after solar + storage is addded to the excel input sheet
        elif property_name == 'solar_plus_storage_mode':
            # check type
            try:
                check_type(self.get(property_name), bool)
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))               
        #==============================================================================            
              
        else:
            print 'No validation method for property %s exists.' % property_name

    def validate(self):
        
        property_names = self.__dict__.keys()
        for property_name in property_names:
            self.validate_property(property_name)
            
        return
        
        
#%%     
class ScenarioSettings(object):
    
    def __init__(self):
        
        self.scen_name = None # type is text, no spaces?
        self.end_year = None
        self.choose_tech = None # if true, multiple techs must be available
        self.region = None
        self.load_growth_scenario = None # valid options only
        self.random_generator_seed = None # int
        
        self.sectors = None # valid options only 
        self.techs = None # valid options only
        self.input_scenario = None # exists on disk
        self.schema = None # string

        self.model_years = None # starts at 2014 and ends <= 2050
        self.tech_mode = None # valid options only

    def set(self, attr, value):

        self.__setattr__(attr, value)
        self.validate_property(attr)
        
        
    def get(self, attr):

        return self.__getattribute__(attr)


    def add_scenario_options(self, scenario_options):
        
        self.set('scen_name', scenario_options['scenario_name'])
        self.set('end_year', scenario_options['end_year'])
        self.set('choose_tech', scenario_options['tech_choice'])
        self.set('region', scenario_options['region'])
        self.set('load_growth_scenario', scenario_options['load_growth_scenario'])
        self.set('random_generator_seed', scenario_options['random_generator_seed'])

    
    def set_tech_mode(self):

        if sorted(self.techs) in [['wind'], ['solar'], ['solar', 'wind']]:
            self.set('tech_mode', 'elec')
        
        elif sorted(self.techs) == ['solar', 'storage']:
            self.set('tech_mode', 'solar+storage')

        elif self.techs == ['du']:
            self.set('tech_mode', 'du')

        elif self.techs == ['ghp']:
            self.set('tech_mode', 'ghp')
            

    def validate_property(self, property_name):
        
        # check not null
        if self.get(property_name) == None:
            raise ValueError('%s has not been set' % property_name)
    
        if property_name == 'scen_name':
            # check type
            try:
                check_type(self.get(property_name), str)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))
            # confirm no spaces
            if ' ' in self.scen_name:
                raise ValueError('Invalid %s: cannot contain spaces.' % property_name)
        
        
        elif property_name == 'end_year':
            try:
                check_type(self.get(property_name), int)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))
            # max of 2050
            if self.end_year > 2050:
                raise ValueError('Invalid %s: end_year must be <= 2050' % property_name)
        
        elif property_name == 'choose_tech':
            try:
                check_type(self.get(property_name), bool)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))  
            # must have multiple techs available
            if self.choose_tech == True and len(self.techs) < 2:
                raise ValueError('Invalid %s: Cannot run tech_choice mode with fewer than two technologies' % property_name)
            # cannot run if tech_mode == 'ghp' or 'du'
            if self.choose_tech == True and self.tech_mode in ('ghp', 'du', 'solar+storage'):
                raise ValueError('Invalid %s: Cannot run tech_choice mode with GHP, DU, or Solar+Storage' % property_name)


        elif property_name == 'region':
            try:
                check_type(self.get(property_name), str)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))  


        elif property_name == 'load_growth_scenario':
            try:
                check_type(self.get(property_name), str)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))  
                

        elif property_name == 'random_generator_seed':
            try:
                check_type(self.get(property_name), int)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))  
                

        elif property_name == 'sectors':
            try:
                check_type(self.get(property_name), dict)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))              
            # check all values are valid
            valid_sectors = set(
                                [('res', 'Residential'),
                                 ('com', 'Commercial'),
                                 ('ind', 'Industrial')]
                             )
            if set(self.sectors.iteritems()).issubset(valid_sectors) == False:
                raise ValueError('Invalid %s: the only allowable sectors are res, com, ind.')   
            # if only ind was selected and tehcmode is ghp or du, do not run
            if self.sectors.keys() == ['ind'] and self.tech_mode in ('ghp', 'du'):
                raise ValueError('Invalid %s: Cannot run industrial sector for %s' % (property_name, self.tech_mode))
                warnings.warn('Industrial sector cannot be modeled for geothermal technologies at this time.')
            # drop 'ind' sector if selected for geo
            if self.sectors.keys() <> ['ind'] and 'ind' in self.sectors.keys() and self.tech_mode in ('ghp', 'du'):
                self.sectors.pop('ind')
                warnings.warn('Industrial sector cannot be modeled for geothermal technologies at this time and will be ignored.')
        
        elif property_name == 'techs':
            try:
                check_type(self.get(property_name), list)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))      
            
            valid_options = [
                                ['wind'],
                                ['solar'],
                                ['du'],
                                ['ghp'],
                                ['solar', 'wind'],
                                ['solar', 'storage']
                               ]
            if sorted(self.techs) not in valid_options:
                raise ValueError("Invalid %s: Cannot currently run that combination of technologies. Valid options are: %s" % (property_name, valid_options))

        elif property_name == 'input_scenario':
            try:
                check_type(self.get(property_name), str)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))            
            # check the path exists
            if os.path.exists(self.input_scenario) == False:
                raise ValueError('Invalid %s: does not exist' % property_name)
  

        elif property_name == 'schema':
            try:
                check_type(self.get(property_name), str)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))  

        elif property_name == 'model_years':
            try:
                check_type(self.get(property_name), list)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))  
            # sort ascending
            self.model_years.sort()
            # make sure starts at 2014
            if self.model_years[0] <> 2014:
                raise ValueError('Invalid %s: Must begin with 2014.' % property_name)
            # last year must be <= 2050
            if self.model_years[-1] > 2050:
                raise ValueError('Invalid %s: End year must be <= 2050.' % property_name)
            
        elif property_name == 'tech_mode':
            try:
                check_type(self.get(property_name), str)            
            except TypeError, e:
                raise TypeError('Invalid %s: %s' % (property_name, e))    
            # check valid options
            valid_options = ['elec',
                             'ghp',
                             'du',
                             'solar+storage']
            if self.tech_mode not in valid_options:
                raise ValueError('Invalid %s: must be one of %s' % (property_name, valid_options))

        else:
            print 'No validation method for property %s exists.' % property_name


    def validate(self):
        
        property_names = self.__dict__.keys()
        for property_name in property_names:
            self.validate_property(property_name)
            
        return
        
        
        
def check_type(obj, expected_type):
    
    if isinstance(obj, expected_type) == False:
        raise TypeError('object type (%s) does not match expected type (%s)' % (type(obj), expected_type))