# -*- coding: utf-8 -*-
"""
Created on Wed May 25 11:16:26 2016

@author: mgleason
"""
import pandas as pd
import numpy as np
from scoop import futures
import pickle
import os
import re 
import warnings
import utility_functions
import operator
from numpy import dtype

class Agent(object):
    
    def __init__(self, data = None, schema = None):
        
        if data is None:
            data = {}
        
        if isinstance(data, pd.core.series.Series):
            self.data = data
        elif isinstance(data, dict):
            self.data = pd.Series(data)

        # initialize attributes        
        self.from_series(self.data)
        
        # TODO(?): add code to assert attribute names and dtypes ()
        self.__assert_attributes__()
        
    def from_series(self, series):
        
        self.data = series
        
        for k, v in self.data.iteritems():
            self.__setattr__(k, v)
        
    def from_dict(self, d):

        self.from_series(pd.Series(d))    
        
        
    def __assert_attributes__(self):
        
        # TODO(?): add code to assert attribute names and dtypes ()
        # note: this is not essential at this point 
        pass
    
    def to_series(self):
        
        return self.data 
        


class Agents(object):
    
    def __init__(self, iterable = None, settings = None):
        
        if iterable is None:
            iterable = []
            
        if isinstance(iterable, list):
            if np.all([str(i.__class__) == str(Agent) for i in iterable]):        
                self.dataframe = pd.concat([i.data for i in iterable], axis = 1, ignore_index = True).T
            elif np.all([isinstance(i, pd.Series) for i in iterable]):   
                self.dataframe = pd.concat(iterable, axis = 1, ignore_index = True).T
            elif np.all([isinstance(i, pd.DataFrame) for i in iterable]):       
                self.dataframe = pd.concat(iterable, axis = 0, ignore_index = True)
            else:
                raise ValueError('iterable must be one of: pandas.DataFrame, list of Agents, list of pandas.Series, or list of pandas.DataFrame')
                
        elif isinstance(iterable, pd.DataFrame):
            self.dataframe = iterable
        
        
    def as_dataframes(self, num_dataframes):

        dataframes = np.array_split(self.dataframe, num_dataframes)
        
        return dataframes
    
    
    def add_agent(self, agent):

        self.dataframe = pd.concat([self.dataframe, pd.DataFrame(agent.data).T], axis = 0, ignore_index = False)
        
        pass


    def get_agents(self):
        
        return [Agent(i[1]) for i in self.dataframe.iterrows()]
    
    
    def get_agent(self, i):
        
        return Agent(self.dataframe.ix[i])
    
    
class AgentSettings(object):
    
    def __init__(self):
        # TODO: create this object
    
        pass
    
class AgentsAlgorithm(object):
    
    def __init__(self, agents, agent_settings = None, in_schema = None, out_schema = None, debug_mode = False, debug_directory = None):
        
        self.agents = agents
        self.agent_settings = agent_settings
        self.debug_mode = debug_mode
        self.debug_directory = debug_directory
        self.in_rows= self.agents.dataframe.shape[0]
        self.agent_settings = agent_settings
        

        if in_schema is None:
            self.in_schema = in_schema
        else:   
            self.in_schema = sorted(in_schema.items(), key = operator.itemgetter(0))
            
        
        if out_schema is None:
            self.out_schema = out_schema
        else:   
            self.out_schema = sorted(out_schema.items(), key = operator.itemgetter(0))



    def __precheck__(self):
        
        if self.in_schema is None:
            warnings.warn("in_schema not specified. Precheck for columns and datatypes will not be performed")
        else:
            actual_schema = sorted(dict(self.agents.dataframe.dtypes).items(), key = operator.itemgetter(0))
            if np.all(np.array(self.in_schema) == np.array(actual_schema)):
                return
            else:
                raise ValueError('precheck failed')
                
    
    def __postcheck__(self, result_agents):
        
        if self.out_schema is None:
            warnings.warn("out_schema not specified. Postcheck for columns and datatypes will not be performed")
        else:
            actual_schema = sorted(dict(result_agents.dataframe.dtypes).items(), key = operator.itemgetter(0))
            if np.all(np.array(self.out_schema) == np.array(actual_schema)):
                return
            else:
                raise ValueError('postcheck failed due to invalid return columns')        
                
        # also check the row count
        result_rows = result_agents.dataframe.shape[0]
        if result_rows <> self.in_rows:
            raise ValueError('postcheck failed due to change in number of agents')
        
    def pickle(self, out_directory):

        out_filename = '%s_%s.pkl' % (utility_functions.current_datetime(), re.sub('objectat', '', re.sub('[<>_ .]','', self.__str__())))
        out_path = os.path.join(out_directory, out_filename)
        pkl = open(out_path, 'wb')
        pickle.dump(self, pkl)
        pkl.close()        
        
    def compute(self, num_workers = 1):
        
        if self.debug_mode == True:
            if self.debug_directory is None:
                warnings.warn('debug_out_dir not specified. Object will not be pickled to disk.', Warning)
            else:
                self.pickle(self.debug_directory)
           
        # perform the pre check
        self.__precheck__()
        
        # split the dataframe up      
        sub_populations = self.agents.as_dataframes(num_workers)
        # farm out jobs
        results = futures.map(self.__do__, sub_populations)
        # recompile results
        result_agents = Agents(pd.concat(results, axis = 0, ignore_index = True))
        
        # perform post check
        self.__postcheck__(result_agents)
        
        return result_agents
        

    def __do__(self, dataframe):
        
        # this is the default -- return the same data that was given        
        return dataframe




##########################################################################################################################################
# FOR TESTING
class SystemSizeSelector(AgentsAlgorithm):

    def __init__(self, *args, **kwargs):
        
        AgentsAlgorithm.__init__(self, *args, **kwargs)
        self.set_in_schema()
        self.set_out_schema()

    def __do__(self, dataframe):
        
        # replace with actual code
        dataframe['system_size_kw'] = 15
        
        return dataframe
    
    # OPTIONAL -- replace code below with "pass" if undesired
    def set_in_schema(self):
        
        # CHANGE THIS
        in_schema = {'pca_reg': dtype('O'), 'cap_cost_multiplier_wind': dtype('float64'), 'county_total_customers_2011': dtype('float64'), 'hdf_load_index': dtype('int64'), 'county_id': dtype('int64'), 'old_county_id': dtype('int64'), 'rate_ids': dtype('O'), 'county_fips': dtype('int64'), 'cap_cost_multiplier_solar': dtype('float64'), 'census_division_abbr': dtype('O'), 'bldg_count_single_fam_res': dtype('int64'), 'acres_per_bldg': dtype('float64'), 'canopy_ht_m': dtype('int64'), 'census_region': dtype('O'), 'cf_bin': dtype('int64'), 'bldg_probs_res': dtype('O'), 'solar_re_9809_gid': dtype('int64'), 'pgid': dtype('int64'), 'county_total_load_mwh_2011': dtype('float64'), 'bldg_count_res': dtype('int64'), 'rate_ranks': dtype('O'), 'state_fips': dtype('int64'), 'state_abbr': dtype('O'), 'i': dtype('int64'), 'j': dtype('int64'), 'canopy_pct': dtype('int64'), 'reeds_reg': dtype('int64'), 'ulocale': dtype('int64')}
        self.in_schema = sorted(in_schema.items(), key = operator.itemgetter(0))  
        
        # OR DELETE ABOVE AND UNCOMMENT BELOW
        #pass
        
    # OPTIONAL -- replace code below with "pass" if undesired
    def set_out_schema(self):

        # CHANGE THIS
        out_schema = {'pca_reg': dtype('O'), 'cap_cost_multiplier_wind': dtype('float64'), 'county_total_customers_2011': dtype('float64'), 'hdf_load_index': dtype('int64'), 'county_id': dtype('int64'), 'old_county_id': dtype('int64'), 'rate_ids': dtype('O'), 'county_fips': dtype('int64'), 'cap_cost_multiplier_solar': dtype('float64'), 'census_division_abbr': dtype('O'), 'bldg_count_single_fam_res': dtype('int64'), 'acres_per_bldg': dtype('float64'), 'canopy_ht_m': dtype('int64'), 'census_region': dtype('O'), 'cf_bin': dtype('int64'), 'system_size_kw': dtype('int64'), 'bldg_probs_res': dtype('O'), 'solar_re_9809_gid': dtype('int64'), 'pgid': dtype('int64'), 'county_total_load_mwh_2011': dtype('float64'), 'bldg_count_res': dtype('int64'), 'rate_ranks': dtype('O'), 'state_fips': dtype('int64'), 'state_abbr': dtype('O'), 'i': dtype('int64'), 'j': dtype('int64'), 'canopy_pct': dtype('int64'), 'reeds_reg': dtype('int64'), 'ulocale': dtype('int64')}
        self.out_schema = sorted(out_schema.items(), key = operator.itemgetter(0)) 
        
        # OR DELETE ABOVE AND UNCOMMENT BELOW
        #pass
        
    
df = pd.read_csv('/Users/mgleason/NREL_Projects/github/diffusion/python/test_agents.csv')
agents = Agents(df)
agent = agents.get_agent(0)

aa = AgentsAlgorithm(agents)
#aa.compute(1)


newagents = SystemSizeSelector(agents, debug_mode = True, debug_directory = '/Users/mgleason/Desktop').compute()



# add compute as default action upon init


