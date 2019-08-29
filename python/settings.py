# -*- coding: utf-8 -*-
"""
Settings class objects.
"""
import os
import multiprocessing
from config import *
import glob
from excel.excel_objects import FancyNamedRange, ExcelError
import pandas as pd
import numpy as np
import json
import shutil
import openpyxl as xl
import sys
import decorators
import warnings

path = os.path.dirname(os.path.abspath(__file__))
par_path = os.path.dirname(path)
sys.path.append(par_path)
import utility_functions as utilfunc

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

def check_type(obj, expected_type):

    if isinstance(obj, expected_type) == False:
        raise TypeError('object type (%s) does not match expected type (%s)' % (
            type(obj), expected_type))

@decorators.fn_timer(logger=logger, tab_level=1, prefix='')
def load_scenario_to_inputSheet(xls_file, model_settings):
     """
     Aggregate agent-level results into ba-level results for the given year.

     Parameters
     ----------
     xls_file : str
          Filepath in the input_scenarios folder.
     model_settings : :class:`settings.ModelSettings`
          Global model settings tariff object.

     Returns
     -------
     settings.ScenarioSettings
          Scenario specific settings and model data loaded from the excel spreadsheet and the associated input csv data folder.
     """
     logger.info('Loading Input Scenario Worksheet')
     #==========================================================================================================
     # Load the spreadsheet and the mapping_file to parse it
     #==========================================================================================================
     try:
          scenarioSettings = ScenarioSettings(xls_file,model_settings)

          if not os.path.exists(xls_file):
               raise ExcelError(
                    'The specified input worksheet (%s) does not exist' % xls_file)

          mapping_file = os.path.join(path,'excel/table_range_lkup_inputSheet.csv')
          if not os.path.exists(mapping_file):
               raise ExcelError(
                    'The required file that maps from named ranges to postgres tables (%s) does not exist' % mapping_file)
          mappings = pd.read_csv(mapping_file)
          mappings['columns'] = mappings['columns'].apply(lambda x: x.split(','))

          with warnings.catch_warnings():
               # ignore meaningless warning
               warnings.filterwarnings(
                    "ignore", message="Discarded range with reserved name")
               wb = xl.load_workbook(xls_file, data_only=True, read_only=True)

     #==========================================================================================================
     # Loop through tables from the mapping_file spreadsheet and convert named ranges to pandas dataframes before loading into scenario settings
     #==========================================================================================================
          for table, range_name, transpose, melt,columns in mappings.itertuples(index=False):
               fnr = FancyNamedRange(wb, range_name)
               if transpose == True:
                    fnr.__transpose_values__()
               if melt == True:
                    fnr.__melt__()
               scenarioSettings.loadFromDataFrame(table, fnr.to_df(columns= columns))
          scenarioSettings.validate()

          return scenarioSettings
     except ExcelError, e:
          raise ExcelError(e)

class ModelSettings(object):
     """
     Class containing the model settings parameters

     Attributes
     ----------
     model_init : float
     cdata : str
     out_dir : str
     start_year : int
     input_scenarios : list
     git_hash : str
     model_path : bool
     local_cores : int
     used_scenario_names : list
     """

     def __init__(self):
          self.model_init = None  # type is float
          self.cdate = None  # type is text
          self.out_dir = None  # doesn't exist already, check parent folder exists
          self.start_year = None  # must = 2016
          self.input_scenarios = None  # type is list, is not empty
          self.git_hash = None  # type is text
          self.model_path = None  # path exists
          self.local_cores = None  # int < cores on machine
          self.used_scenario_names = [] #running list of modeled scenarios

     def check_scenario_name(self,scenario_settings):
          """
          Check if `ModelSettings.scenario_name` is in `ModelSettings.used_scenario_names`
          """
          if scenario_settings.scenario_name in self.used_scenario_names:
               i = 1
               new_scen_name = "%s_%s" % (scenario_settings.scenario_name, i)
               while new_scen_name not in self.used_scenario_names:
                    i+1
                    new_scen_name = "%s_%s" % (scenario_settings.scenario_name, i)
               logger.info("Warning: Scenario name %s is a duplicate. Renaming to %s" % (scenario_settings.scenario_name, new_scen_name))
               scenario_settings.scenario_name = new_scen_name

          self.used_scenario_names.append(scenario_settings.scenario_name)

          return scenario_settings.scenario_name

     def set(self, attr, value):
          self.__setattr__(attr, value)
          self.validate_property(attr)

     def get(self, attr):
          return self.__getattribute__(attr)

     def validate_property(self, property_name):
          """Check if property is not null"""
          if self.get(property_name) == None:
               raise ValueError('%s has not been set' % property_name)

          # validation for specific properties
          if property_name =='used_scenario_names':
               pass

          elif property_name == 'model_init':
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
               # assert equals 2016
               if self.start_year <> 2016:
                    raise ValueError(
                    'Invalid %s: must be set to 2016' % property_name)

          elif property_name == 'input_scenarios':
               # check type
               try:
                    check_type(self.get(property_name), list)
               except TypeError, e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))
               if len(self.input_scenarios) == 0:
                    raise ValueError(
                    "Invalid %s: No input scenario spreadsheet were found in the input_scenarios folder." % property_name)

          elif property_name == 'git_hash':
               # check type
               try:
                    check_type(self.get(property_name), str)
               except TypeError, e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

          elif property_name == 'role':
               # check type
               try:
                    check_type(self.get(property_name), str)
               except TypeError, e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

          elif property_name == 'model_path':
               # check type
               try:
                    check_type(self.get(property_name), str)
               except TypeError, e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))
               # check the path exists
               if os.path.exists(self.model_path) == False:
                    raise ValueError('Invalid %s: does not exist' % property_name)

          elif property_name == 'local_cores':
               # check type
               try:
                    check_type(self.get(property_name), int)
               except TypeError, e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

               # check if too large
               if self.local_cores > multiprocessing.cpu_count():
                    raise ValueError(
                    'Invalid %s: value exceeds number of CPUs on local machine' % property_name)

          elif property_name == 'input_agent_dir':

               if not os.path.exists(self.input_agent_dir):
                    raise TypeError('Invalid %s: %s' % (property_name, "{} does not exist".format(self.input_agent_dir)))

          else:
               print 'No validation method for property %s exists.' % property_name

     def validate(self):
          property_names = self.__dict__.keys()
          for property_name in property_names:
               self.validate_property(property_name)

          return

class SectorInputs:
     """Storage of sector-specific scenario inputs"""
     def __init__(self,name):
          self.sector_abbr = name
          self.sector_name = SECTOR_NAMES[name]
          self.rate_structure_name = None
          self.rate_escalation_name = None
          self.max_market_curve_name = None

class ScenarioSettings:
     """Storage of all scenario specific inputs"""
     def __init__(self, input_scenario, model_settings, time_step_increment=2):
          self.input_scenario = input_scenario
          self.time_step_increment = time_step_increment
          self.scenario_name = None
          self.scenario_folder = None
          self.generate_agents = True
          self.agents_file_name = ''
          self.techs = None
          self.tech_mode = None
          self.region_name = None
          self.sectors = []
          self.sector_data = {}
          self.scenarios = {
               'load_growth_scenario_name': None,
               'carbon_price_scenario_name': None,
               'compensation_scenario_name':None
               }
          self.random_seed_generator = None
          self.start_year = model_settings.start_year
          self.end_year = None
          self.out_dir = model_settings.out_dir
          self.pv_trajectories = pd.DataFrame()
          self.storage_trajectories = pd.DataFrame()
          self.financial_trajectories = pd.DataFrame()
          self.control_reg_trajectories = pd.DataFrame()
          self.market_trajectories = pd.DataFrame()
          self.state_start_conditions = pd.DataFrame()
          self.storage_options = {}
          self.financial_options = {}

     @property
     def input_csv_folder(self):
          """Location of input csv files"""
          return os.path.join('../input_scenarios',self.scenario_folder)

     @property
     def model_years(self):
          """Range of years to model"""
          if self.start_year and self.end_year:
               return range(self.start_year, self.end_year + 1, self.time_step_increment)
          else:
               return []

     def validate(self):
          #==========================================================================================================
          # Validate the scenario attributes for data type and necessary file system structure
          #==========================================================================================================

          def check_type(obj, expected_type):
               if expected_type == str:
                    failed = not (isinstance(obj, str) or isinstance(obj, unicode))
               else:
                    failed = not isinstance(obj, expected_type)
               if failed:
                    raise TypeError('object type (%s) does not match expected type (%s)' % (type(obj), expected_type))

          #==========================================================================================================
          # Format list of inputs to read from SectorInput Objects in a following loop
          #==========================================================================================================
          sector_strs = []
          for s in self.sectors:
               for a in ['rate_structure_name', 'rate_escalation_name', 'max_market_curve_name']:
                    sector_strs.append('sector_data.{}.{}'.format(s,a))
          #==========================================================================================================
          # Format full list inputs to read from SectorInput Objects in the following loop
          #==========================================================================================================
          check_data_types = {
               str: [     'scenario_name',
                          'scenario_folder',
                              'scenarios.load_growth_scenario_name',
                              'scenarios.carbon_price_scenario_name',
                              'scenarios.compensation_scenario_name',
                              'tech_mode'] + sector_strs,
               int: ['start_year','end_year'],
               list: ['sectors']
          }
          #==========================================================================================================
          # Loop through and validate all inputs
          #==========================================================================================================
          for data_type,attributes in check_data_types.items():
               for a in attributes:
                    if '.' in a:
                         levels = a.split('.')
                         base = getattr(self, levels[0])
                         v = base.get(levels[1])
                         if levels[0] == 'sector_data':
                              v = getattr(v, levels[2])
                    else:
                         v = getattr(self,a)
                    try:
                         check_type(v, data_type)
                    except TypeError, e:
                         raise TypeError('Invalid %s: %s' % (a, e))
          #==========================================================================================================
          # Check other one-off checks
          #==========================================================================================================
          if ' ' in self.scenario_name:
               raise ValueError( 'Invalid %s: cannot contain spaces.' % property_name)
          if self.end_year > 2050:
               raise ValueError('Invalid: end_year must be <= 2050' )
          if os.path.exists(self.input_scenario) == False:
               raise ValueError('Invalid %s: does not exist' % self.input_scenario)
          if os.path.exists(os.path.join('../input_agents',self.agents_file_name)) == False:
               raise ValueError('Invalid %s: does not exist' % os.path.join('../input_agents',self.agents_file_name))
          # make sure starts at 2016
          self.model_years.sort()
          if self.model_years[0] <> 2016:
               raise ValueError('Invalid %s: Must begin with 2016.' % 'model_years')
          # last year must be <= 2050
          if self.model_years[-1] > 2050:
               raise ValueError('Invalid %s: End year must be <= 2050.' % 'model_years')
          # tech is a valid value
          if self.techs not in TECHS:
               raise ValueError('Invalid %s: must be one of %s' %(self.techs, TECHS))
          # tech_mode is a valid value
          if self.tech_mode not in TECH_MODES:
               raise ValueError('Invalid %s: must be one of %s' %(self.tech_mode, TECHMODES))

     def write_folders(self, model_settings):
          """Make output folders for the run"""
          self.scenario_name = model_settings.check_scenario_name(self)
          self.out_scen_path = os.path.join(self.out_dir, self.scenario_name)
          self.dir_to_write_input_data = self.out_scen_path + '/input_data'
          os.makedirs(self.out_scen_path)
          os.makedirs(self.dir_to_write_input_data)
          shutil.copy(self.input_scenario, self.out_scen_path)

     def write_inputs(self):
          """Export key attributes to output folder"""
          self.pv_trajectories.to_csv(self.dir_to_write_input_data + '/pv_trajectories.csv', index=False)
          self.storage_trajectories.to_csv(self.dir_to_write_input_data + '/storage_trajectories.csv', index=False)
          self.financial_trajectories.to_csv(self.dir_to_write_input_data + '/financial_trajectories.csv', index=False)
          self.control_reg_trajectories.to_csv(self.dir_to_write_input_data + '/control_reg_trajectories.csv', index=False)
          self.market_trajectories.to_csv(self.dir_to_write_input_data + '/market_trajectories.csv', index=False)
          self.state_start_conditions.to_csv(self.dir_to_write_input_data + '/state_start_conditions.csv', index=False)

     def collapse_sectors(self, df, columns, adders =[]):
          """Split each row into groups by sector, then stack the groups"""
          adders  = ['year'] + adders
          result = pd.DataFrame()
          for sector in self.sectors:
               rename_set = {k.format(sector):v for k,v in columns.items()}
               tmp = df[rename_set.keys() + adders]
               tmp.rename(columns=rename_set, inplace=True)
               tmp['sector_abbr'] = sector
               result = pd.concat([result,tmp])
          return result

     def loadFromDataFrame(self,table_name,df):
          """Accept a dataframe from the input spreadsheet and load it into the scenario settings"""

          if table_name == "input_main_scenario_options":
               values = df.iloc[0]
              #==========================================================================================================
              # determine if agents need to be generated or loaded from an existing pickle
              #==========================================================================================================
               if str(values.get('agents_file')).replace(' ','') not in ['None','','0']:
                  self.agents_file_name = os.path.join('../input_agents',values.get('agents_file'))
                  self.generate_agents = False
              #==========================================================================================================
              # parse other key attributes
              #==========================================================================================================
               self.scenario_name = values.get('scenario_name')
               self.scenario_folder = values.get('scenario_folder')
               self.end_year = values.get('end_year')
               self.scenarios['load_growth_scenario_name'] = values.get('load_growth_scenario')
               self.scenarios['carbon_price_scenario_name'] = values.get('carbon_price')
               self.scenarios['compensation_scenario_name'] = values.get('nem_scenario')
               sector_selection = values.get('markets')
               self.sector_data = {}
               if sector_selection == 'All':
                   self.sectors = SECTORS
               else:
                   self.sectors = [i for i in SECTORS if i in sector_selection.lower()]
               for s in self.sectors:
                   sector = SectorInputs(s)
                   sector.rate_structure_name = values.get(s + '_rate_structure')
                   sector.rate_escalation_name = values.get(s + '_rate_escalation')
                   sector.max_market_curve_name = values.get(s + '_max_market_curve')
                   self.sector_data[s] = sector

               self.techs = ['solar']
               self.tech_mode = 'elec'
              #==========================================================================================================
              # load from input csv data folder
              #==========================================================================================================
               self.load_max_market_share()
               self.load_load_growth()
               self.load_rate_escalations()
               self.load_bass_params()
               self.load_wholesale_electricity()
               self.load_avoided_costs()
               self.load_nem_settings()

          if table_name == "input_main_market_inflation":
               self.financial_options['annual_inflation_pct'] = df.iloc[0].get('ann_inflation')

          if table_name == 'input_main_storage_options':
               self.storage_options['batt_replacement_yr'] = df.iloc[0].get('batt_replacement_yr')
               self.storage_options['batt_replacement_frac_kw'] = df.iloc[0].get('batt_replace_frac_kw')
               self.storage_options['batt_replacement_frac_kwh'] = df.iloc[0].get('batt_replace_frac_kwh')

          if table_name == "input_main_pv_trajectories":
               rename_set = {'pv_price_{}':'pv_price_per_kw','pv_om_{}':'pv_om_per_kw','pv_variable_om_{}':'pv_variable_om_per_kw','pv_power_density_w_per_sqft_{}':'pv_power_density_w_per_sqft','pv_deg_{}':'pv_deg'}
               result = self.collapse_sectors(df, rename_set)

               if self.pv_trajectories.empty:
                    self.pv_trajectories = result
               else:
                    self.pv_trajectories = self.pv_trajectories.merge(result, on=['year','sector_abbr'])

          if table_name == "input_main_storage_trajectories":
               rename_set = {'batt_price_per_kwh_{}':'batt_price_per_kwh','batt_price_per_kw_{}':'batt_price_per_kw','batt_om_per_kw_{}':'batt_om_per_kw','batt_om_per_kwh_{}':'batt_om_per_kwh'}
               result = self.collapse_sectors(df, rename_set)

               if self.storage_trajectories.empty:
                    self.storage_trajectories = result
               else:
                    self.storage_trajectories = self.storage_trajectories.merge(result, on=['year','sector_abbr'])

          if table_name == "input_main_depreciation_schedule":
               rename_set = {'1_{}':'1','2_{}':'2','3_{}':'3','4_{}':'4','5_{}':'5','6_{}':'6'}
               result = self.collapse_sectors(df, rename_set)
               columns = ['1', '2', '3', '4', '5', '6']
               result['deprec_sch']=result.apply(lambda x: [x.to_dict()[y] for y in columns], axis=1)
               result  = result[['year','sector_abbr','deprec_sch']]
               if self.financial_trajectories.empty:
                    self.financial_trajectories = result
               else:
                    self.financial_trajectories = self.financial_trajectories.merge(result, on=['year','sector_abbr'])

          if table_name == "input_main_financial_trajecories":
               self.financing_terms = df
               rename_set = {'loan_term_{}':'loan_term', 'tax_rate_{}':'tax_rate','itc_fraction_{}':'itc_fraction'}
               adders = ['economic_lifetime']
               result = self.collapse_sectors(df, rename_set, adders)
               result['tech']='solar'
               # result['min_size_kw']= -1
               # result['max_size_kw']= None
               if self.financial_trajectories.empty:
                    self.financial_trajectories = result
               else:
                    self.financial_trajectories = self.financial_trajectories.merge(result, on=['year','sector_abbr'])

          if table_name =="input_main_market_projections":
               adders = ['default_rate_escalations','carbon_dollars_per_ton']
               rename_set = {'user_defined_{}_rate_escalations':'user_defined_rate_escalations'}
               result = self.collapse_sectors(df, rename_set, adders)
               if self.market_trajectories.empty:
                    self.market_trajectories = result
               else:
                    self.market_trajectories = self.market_trajectories.merge(result, on=['year','sector_abbr'])

          if 'carbon_dollars_per_ton' in self.market_trajectories.columns and 'carbon_price_cents_per_kwh' not in self.control_reg_trajectories.columns:
               self.load_carbon_intensities()

          return

     def load_nem_settings(self):
          df = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'nem_settings.csv'),index_col=None)
     
          # --- Check available columns in loaded csv ---
          if 'state_id' in df.columns:
               on = 'state_id'
          elif 'control_reg_id' in df.columns:
               on = 'control_reg_id'
          elif 'tariff_class' in df.columns:
               on = 'tariff_class'
          else:
               raise KeyError("'state_id' and 'control_reg_id' not in nem_settings.csv columns")

          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = df
          else:
               columns = [on, 'sector_abbr', 'year']
               self.control_reg_trajectories = self.control_reg_trajectories.merge(df, on=columns)

     def load_core_agent_attributes(self):
          self.core_agent_attributes = pd.DataFrame()
          tmp = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder, 'agent_core_attributes_all.csv'),index_col=None)
          tmp = tmp.sample(frac=SAMPLE_PCT) #sample (i.e. for test runs) a smaller agent_df, defined in config
          tmp['agent_id']= range(tmp.shape[0])
          for t in self.techs:
               tmp['tech'] = t
          self.core_agent_attributes = pd.concat([self.core_agent_attributes,tmp])
          if 'solar' in self.techs:
               df = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder, 'pv_state_starting_capacities.csv'),index_col=None)
               self.core_agent_attributes = self.core_agent_attributes.merge(df, on=['control_reg_id','state_id','sector_abbr','tariff_class'])
          
          # There was a problem where an agent was being generated that had no customers in the bin, but load in the bin
          # This is a temporary patch to get the model to run in this scenario\
          self.core_agent_attributes['customers_in_bin'] = np.where(self.core_agent_attributes['customers_in_bin']==0, 1, self.core_agent_attributes['customers_in_bin'])
          self.core_agent_attributes['load_per_customer_in_bin_kwh'] = np.where(self.core_agent_attributes['load_per_customer_in_bin_kwh']==0, 1, self.core_agent_attributes['load_per_customer_in_bin_kwh'])

     def load_normalized_load_profiles(self):
          df = pd.read_json(os.path.join(self.input_csv_folder, 'normalized_load.json'))
          df = df.rename(columns={'kwh':'consumption_hourly'})

          # --- Check available columns in loaded csv ---
          if 'state_id' in df.columns:
               on = 'state_id'
          elif 'control_reg_id' in df.columns:
               on = 'control_reg_id'
          elif 'tariff_class' in df.columns:
               on = 'tariff_class'
          else:
               raise KeyError("'state_id' and 'control_reg_id' not in normalized_load.csv columns")

          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=[on])

          # in_cols = self.core_agent_attributes.columns

          # def scale_array_precision(row, array_col, prec_offset_col):
          #      row[array_col] = np.array(
          #      row[array_col], dtype='float64') / row[prec_offset_col]
          #      return row

          # def scale_array_sum(row, array_col, scale_col):
          #      hourly_array = np.array(row[array_col], dtype='float64')
          #      row[array_col] = hourly_array / hourly_array.sum() * np.float64(row[scale_col])
          #      return row

          # # apply the scale offset to convert values to float with correct precision
          # self.core_agent_attributes = self.core_agent_attributes.apply(scale_array_precision, axis=1, args=('consumption_hourly', 'scale_offset_load'))

          # # scale the normalized profile to sum to the total load
          # self.core_agent_attributes = self.core_agent_attributes.apply(scale_array_sum, axis=1, args=('consumption_hourly', 'load_per_customer_in_bin_kwh'))

          # # subset to only the desired output columns
          # out_cols = list(in_cols.values)
          # self.core_agent_attributes = self.core_agent_attributes[out_cols]

     def load_interconnection_settings(self):
          """Load maximum interconnection limits from csv"""
          df = pd.read_csv(os.path.join(self.input_csv_folder, 'interconnection_limits.csv'), index_col=None)
          df = [['control_reg_id','state_id','interconnection_limit_kw']]
          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=['state_id','control_reg_id'])

     def load_normalized_hourly_resource_solar(self):
          df = pd.read_json(os.path.join(self.input_csv_folder,'solar_resource_hourly.json'))
          df = df.rename(columns={'cf':'solar_cf_profile'})

          # --- Check available columns in loaded csv ---
          if 'state_id' in df.columns:
               on = 'state_id'
          elif 'control_reg_id' in df.columns:
               on = 'control_reg_id'
          elif 'tariff_class' in df.columns:
               on = 'tariff_class'
          else:
               raise KeyError("'state_id' and 'control_reg_id' not in solar_resource_hourly.json columns")

          df = df[[on,'solar_cf_profile']]
          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=[on])

          # if 'solar' in self.techs:
          #      df['scale_offset_solar'] = 1e3
          #      self.core_agent_attributes = self.core_agent_attributes.merge(df, on=[on])
          # else:
          #      self.core_agent_attributes['scale_offset'] = None
          #      self.core_agent_attributes['generation_hourly'] = None
          # self.core_agent_attributes['solar_cf_profile'] = self.core_agent_attributes['generation_hourly']

     def load_electric_rates_json(self):
          df = pd.read_json(os.path.join(self.input_csv_folder,'urdb3_rates.json'))
          df = df[['rate_id_alias','rate_json']]
          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=['rate_id_alias'])
          self.core_agent_attributes.rename(columns={'rate_json':'tariff_dict', 'rate_id_alias':'tariff_id'}, inplace=True)
          self.core_agent_attributes['tariff_dict'] = self.core_agent_attributes['tariff_dict'].apply(lambda x: json.loads(x))

     def load_carbon_intensities(self):
          set_zero = False
          if self.scenarios['carbon_price_scenario_name'] == 'Price Based On State Carbon Intensity':
               df = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'carbon_intensities_grid.csv'),index_col=None)

          elif self.scenarios['carbon_price_scenario_name'] in ['Price Based On NG Offset','No Carbon Price']:
               df = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'carbon_intensities_ng.csv'),index_col=None)

          if self.scenarios['carbon_price_scenario_name'] == 'No Carbon Price':
               set_zero = True
          ids = ['control_reg_id']
          years = [i for i in df.columns if i not in ids]
          result = pd.DataFrame()
          for year in years:
            tmp = df[ids+[year]]
            if set_zero:
              tmp[year] = 0
            tmp['year'] = int(year)
            tmp.rename(columns={year:'t_co2_per_kwh'},inplace=True)
            result = pd.concat([result,tmp])

          result = result.merge(self.market_trajectories[['year','carbon_dollars_per_ton','sector_abbr']], on=['year'])
          result['carbon_price_cents_per_kwh'] = result['t_co2_per_kwh'] * 100 * result['carbon_dollars_per_ton']

          self.control_reg_trajectories = self.control_reg_trajectories.merge(result, on=['control_reg_id','year','sector_abbr'])

     def load_max_market_share(self):
          view = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'max_market_share_settings.csv'),index_col=None)

          mms_filter = []
          for sector,settings  in self.sector_data.items():
               mms_filter.append( list((view['sector_abbr'] == sector) & ( view['source'] == settings.max_market_curve_name ) & (view['business_model'] == 'host_owned')))
               mms_filter.append( list((view['sector_abbr'] == sector) & ( view['source'] == "NREL" ) & (view['business_model'] == 'tpo')))

          df = view[np.any(mms_filter,axis=0)]

          df_selection = df[(df['metric_value']==30) & (df['metric']=='payback_period') & (df['business_model']=='host_owned')]
          df_selection['metric_value'] = 30.1
          df_selection['max_market_share'] = 0
          self.market_share_parameters = pd.concat([df,df_selection])

     def load_load_growth(self):
          view = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'load_growth_projections.csv'),index_col=None)
          view.rename(columns={'scenario':'load_growth_scenario'}, inplace=True)
          re_filter = []
          for sector,settings  in self.sector_data.items():
               re_filter.append( list((view['sector_abbr'] == settings.sector_abbr.lower()) & ( view['load_growth_scenario'] == self.scenarios['load_growth_scenario_name']) ))
          view = view[np.any(re_filter,axis=0)]

          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = view
          else:
               columns = ['control_reg_id', 'sector_abbr', 'year']
               self.control_reg_trajectories = self.control_reg_trajectories.merge(view, on=columns)

     def load_compensation_settings(self):
          df = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'nem_settings.csv'),index_col=None)

          # --- Load Correct Scenario Settings --- 
          if self.scenarios['compensation_scenario_name'] == 'Buy All Sell All':
               self.core_agent_attributes['compensation_style'] = 'Buy All Sell All'
          elif self.scenarios['compensation_scenario_name'] == 'Net Billing (Wholesale)':
               self.core_agent_attributes['compensation_style'] = 'Net Billing (Wholesale)'
          elif self.scenarios['compensation_scenario_name'] == 'Net Billing (Avoided Cost)':
               self.core_agent_attributes['compensation_style'] = 'Net Billing (Avoided Cost)'
          elif self.scenarios['compensation_scenario_name'] == 'Net Metering':
               self.core_agent_attributes['compensation_style'] = 'Net Metering'


     def load_financing_rates(self):
          df = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'financing_rates.csv'), index_col=None)

          # --- Check available columns in loaded csv ---
          if 'state_id' in df.columns:
               on = 'state_id'
          elif 'control_reg_id' in df.columns:
               on = 'control_reg_id'
          else:
               raise KeyError("'state_id' and 'control_reg_id' not in financing_rates.csv columns")
          self.core_agent_attributes = pd.merge(self.core_agent_attributes, df, on=[on,'sector_abbr'])

     def load_avoided_costs(self):
          df = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'avoided_cost_rates.csv'), encoding='utf-8-sig',index_col=None)
          ids = ['control_reg_id']
          years = [i for i in df.columns if i not in ids]
          result = pd.DataFrame()
          for year in years:
               tmp = df[ids+[year]]
               tmp['year'] = int(year)
               tmp.rename(columns={year:'hourly_excess_sell_rate_usd_per_kwh'},inplace=True)
               result = pd.concat([result,tmp])
          
          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = result
          else:
               columns = ['control_reg_id', 'year']
               self.control_reg_trajectories = self.control_reg_trajectories.merge(result, on=columns)

     def load_wholesale_electricity(self):
          df = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'wholesale_rates.csv'),index_col=None)
          ids = ['control_reg_id']
          years = [i for i in df.columns if i not in ids]
          result = pd.DataFrame()
          for year in years:
            tmp = df[ids+[year]]
            tmp['year'] = int(year)
            tmp.rename(columns={year:'wholesale_elec_usd_per_kwh'},inplace=True)
            result = pd.concat([result,tmp])

          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = result
          else:
               columns = ['control_reg_id', 'year']
               self.control_reg_trajectories = self.control_reg_trajectories.merge(result, on=columns)

     def load_rate_escalations(self):
          view = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'rate_escalations.csv'),index_col=None)
          view.rename(columns={'escalation_factor':'elec_price_multiplier'}, inplace=True)
          re_filter = []
          for sector,settings  in self.sector_data.items():
               re_filter.append( list((view['sector_abbr'] == settings.sector_abbr.lower()) & ( view['source'] == settings.rate_escalation_name ) ))
          view = view[np.any(re_filter,axis=0)]

          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = view
          else:
               columns = ['control_reg_id', 'sector_abbr', 'year']
               self.control_reg_trajectories = self.control_reg_trajectories.merge(view, on= columns)

     def load_bass_params(self):
          view = pd.DataFrame.from_csv(os.path.join(self.input_csv_folder,'pv_bass.csv'),index_col=None)
          columns = ['control_reg_id','state','sector_abbr']

          if self.state_start_conditions.empty:
               self.state_start_conditions = view
          else:
               self.state_start_conditions = self.state_start_conditions.merge(view, on= columns)

     def get_pv_specs(self):
          return self.pv_trajectories[['year','sector_abbr','pv_power_density_w_per_sqft','pv_deg','pv_price_per_kw','pv_om_per_kw','pv_variable_om_per_kw']]

     def get_batt_price_trajectories(self):
          return self.storage_trajectories[['year', 'sector_abbr','batt_price_per_kwh','batt_price_per_kw','batt_om_per_kw','batt_om_per_kwh']]

     def get_financing_terms(self):
          return self.financial_trajectories[['year','sector_abbr','deprec_sch','loan_term','itc_fraction','tax_rate','economic_lifetime']]

     def get_rate_escalations(self):
          return self.control_reg_trajectories[['control_reg_id', 'sector_abbr','elec_price_multiplier','year']]

     def get_wholesale_elec_prices(self):
          return self.control_reg_trajectories[['control_reg_id', 'sector_abbr','wholesale_elec_usd_per_kwh','year']]

     def get_load_growth(self,year):
          return self.control_reg_trajectories[self.control_reg_trajectories['year']==year][['control_reg_id', 'sector_abbr','load_multiplier']]

     def get_nem_settings(self,year):
          return self.control_reg_trajectories[self.control_reg_trajectories['year']==year][['control_reg_id','sector_abbr','nem_system_size_limit_kw','wholesale_elec_usd_per_kwh','hourly_excess_sell_rate_usd_per_kwh']]

     def get_carbon_intensities(self,year):
          return self.control_reg_trajectories[self.control_reg_trajectories['year']==year][['control_reg_id', 'sector_abbr','carbon_price_cents_per_kwh']]

     def get_max_market_share(self):
          return self.market_share_parameters

     def get_bass_params(self):
          return self.state_start_conditions[['control_reg_id', 'sector_abbr','state_id','p','q','teq_yr1','tech']]

def init_model_settings():
    """initialize Model Settings object (this controls settings that apply to all scenarios to be executed)"""
    model_settings = ModelSettings()
    #==========================================================================================================
    # add the config to model settings; set model starting time, and output directory based on run time
    #==========================================================================================================
    model_settings.set('start_year', START_YEAR)
    model_settings.set('model_path', MODEL_PATH)
    model_settings.set('local_cores', LOCAL_CORES)
    model_settings.set('model_init', utilfunc.get_epoch_time())
    datetime = utilfunc.get_formatted_time()
    print(datetime)
    output_dir = str(raw_input('Run name (default of formatted_time):')) or datetime
    print(output_dir)
    model_settings.set('cdate', datetime)
    model_settings.set('out_dir',  '%s/runs/results_%s' % (os.path.dirname(os.getcwd()), output_dir))
    model_settings.set('input_agent_dir', '%s/input_agents' % os.path.dirname(os.getcwd()))
    model_settings.set('git_hash', utilfunc.get_git_hash())
    model_settings.set('input_scenarios', [s for s in glob.glob("../input_scenarios/*.xls*") if not '~$' in s])

    #==========================================================================================================
    # validate model settings and make the ouput directory
    #==========================================================================================================
    model_settings.validate()
    os.makedirs(model_settings.out_dir)

    return model_settings

def init_scenario_settings(scenario_file, model_settings):
     """load scenario specific data and configure output settings"""
#     try:
     scenario_settings = load_scenario_to_inputSheet(scenario_file, model_settings)
     scenario_settings.write_folders(model_settings)
     scenario_settings.write_inputs()
     scenario_settings.validate()

#     except Exception, e:
#         print e
#         raise Exception('\tLoading failed with the following error: %s' % e)

     return scenario_settings 
