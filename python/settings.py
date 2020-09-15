# -*- coding: utf-8 -*-
"""
Settings class objects.
"""
import os
import multiprocessing
from config import *
import glob
import ast
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

#==============================================================================
# Load logger
import utility_functions as utilfunc
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
               scenarioSettings.loadFromDataFrame(table, fnr.to_df(columns=columns))
          scenarioSettings.validate()

          return scenarioSettings
     except ExcelError as e:
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
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

          elif property_name == 'cdate':
               # check type
               try:
                    check_type(self.get(property_name), str)
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

          elif property_name == 'out_dir':
               # check type
               try:
                    check_type(self.get(property_name), str)
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

          elif property_name == 'start_year':
               # check type
               try:
                    check_type(self.get(property_name), int)
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))
               # assert equals 2016
               if self.start_year != 2016:
                    raise ValueError(
                    'Invalid %s: must be set to 2016' % property_name)

          elif property_name == 'input_scenarios':
               # check type
               try:
                    check_type(self.get(property_name), list)
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))
               if len(self.input_scenarios) == 0:
                    raise ValueError(
                    "Invalid %s: No input scenario spreadsheet were found in the input_scenarios folder." % property_name)

          elif property_name == 'git_hash':
               # check type
               try:
                    check_type(self.get(property_name), str)
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

          elif property_name == 'role':
               # check type
               try:
                    check_type(self.get(property_name), str)
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

          elif property_name == 'model_path':
               # check type
               try:
                    check_type(self.get(property_name), str)
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))
               # check the path exists
               if os.path.exists(self.model_path) == False:
                    raise ValueError('Invalid %s: does not exist' % property_name)

          elif property_name == 'local_cores':
               # check type
               try:
                    check_type(self.get(property_name), int)
               except TypeError as e:
                    raise TypeError('Invalid %s: %s' % (property_name, e))

               # check if too large
               if self.local_cores > multiprocessing.cpu_count():
                    raise ValueError(
                    'Invalid %s: value exceeds number of CPUs on local machine' % property_name)


          else:
               print('No validation method for property %s exists.' % property_name)

     def validate(self):
          property_names = list(self.__dict__.keys())
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
          return os.path.join(os.pardir, 'input_scenarios',self.scenario_folder)

     @property
     def model_years(self):
          """Range of years to model"""
          if self.start_year and self.end_year:
               return list(range(self.start_year, self.end_year + 1, self.time_step_increment))
          else:
               return []

     def validate(self):
          #==========================================================================================================
          # Validate the scenario attributes for data type and necessary file system structure
          #==========================================================================================================

          def check_type(obj, expected_type):
               if expected_type == str:
                    failed = not (isinstance(obj, str))
               elif expected_type == int:
                    failed = not isinstance(int(obj), expected_type)
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
                              'scenarios.compensation_scenario_name',
                              'tech_mode'] + sector_strs,
               int: ['start_year','end_year'],
               list: ['sectors']
          }
          #==========================================================================================================
          # Loop through and validate all inputs
          #==========================================================================================================
          for data_type,attributes in list(check_data_types.items()):
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
                    except TypeError as e:
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

          # make sure starts at 2016
          self.model_years.sort()
          if self.model_years[0] != 2016:
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
          self.dir_to_write_input_data = os.path.join(self.out_scen_path, 'input_data')
          os.makedirs(self.out_scen_path)
          os.makedirs(self.dir_to_write_input_data)
          shutil.copy(self.input_scenario, self.out_scen_path)

     def write_inputs(self):
          """Export key attributes to output folder"""
          self.pv_trajectories.to_csv(os.path.join(self.dir_to_write_input_data, 'pv_trajectories.csv'), index=False)
          self.storage_trajectories.to_csv(os.path.join(self.dir_to_write_input_data, 'storage_trajectories.csv'), index=False)
          self.financial_trajectories.to_csv(os.path.join(self.dir_to_write_input_data, 'financial_trajectories.csv'), index=False)
          self.control_reg_trajectories.to_csv(os.path.join(self.dir_to_write_input_data, 'control_reg_trajectories.csv'), index=False)
          self.market_trajectories.to_csv(os.path.join(self.dir_to_write_input_data, 'market_trajectories.csv'), index=False)
          self.state_start_conditions.to_csv(os.path.join(self.dir_to_write_input_data, 'state_start_conditions.csv'), index=False)

     def collapse_sectors(self, df, columns, adders =[]):
          """Split each row into groups by sector, then stack the groups"""
          adders  = ['year'] + adders
          result = pd.DataFrame()
          for sector in self.sectors:
               rename_set = {k.format(sector):v for k,v in list(columns.items())}
               tmp = df[list(rename_set.keys()) + adders]
               tmp.rename(columns=rename_set, inplace=True)
               tmp['sector_abbr'] = sector
               result = pd.concat([result,tmp], sort=False)
          return result

     def loadFromDataFrame(self,table_name,df):
          """Accept a dataframe from the input spreadsheet and load it into the scenario settings"""

          if table_name == "input_main_scenario_options":
               values = df.iloc[0]
              #==========================================================================================================
              # determine if agents need to be generated or loaded from an existing pickle
              #==========================================================================================================
               if str(values.get('agents_file')).replace(' ','') not in ['None','','0', 'nan']:
                  self.generate_agents = False
              #==========================================================================================================
              # parse other key attributes
              #==========================================================================================================
               self.scenario_name = values.get('scenario_name')
               self.scenario_folder = values.get('scenario_folder')
               self.end_year = values.get('end_year')
               self.scenarios['load_growth_scenario_name'] = values.get('load_growth_scenario')
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
               adders = ['default_rate_escalations']
               rename_set = {'user_defined_{}_rate_escalations':'user_defined_rate_escalations'}
               result = self.collapse_sectors(df, rename_set, adders)
               if self.market_trajectories.empty:
                    self.market_trajectories = result
               else:
                    self.market_trajectories = self.market_trajectories.merge(result, on=['year','sector_abbr'])

          return

     def _find_geography_column_to_merge_on(self, df):
          on = []
          for cat in ['district_id','state_id','control_reg_id','tariff_id','sector_abbr','year']:
               if cat in df.columns:
                    on.append(cat)
          return on

     def load_nem_settings(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder,'nem_settings.csv'),index_col=None)
          on = self._find_geography_column_to_merge_on(df)
          self.control_reg_trajectories = self.control_reg_trajectories.merge(df, on=on)

     def load_core_agent_attributes(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder, 'agent_core_attributes.csv'), index_col=None)
          df = df.sample(frac=SAMPLE_PCT)
          df['agent_id'] = list(range(df.shape[0]))
          self.core_agent_attributes = df

     def load_starting_capacities(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder, 'pv_state_starting_capacities.csv'),index_col=None)
          on = self._find_geography_column_to_merge_on(df)
          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=on)

     def load_normalized_load_profiles(self):
          if os.path.exists(os.path.join(self.input_csv_folder, 'normalized_load.json')):
               df = pd.read_json(os.path.join(self.input_csv_folder, 'normalized_load.json'))
          elif os.path.exists(os.path.join(self.input_csv_folder, 'normalized_load.csv')):
               df = pd.read_csv(os.path.join(self.input_csv_folder, 'normalized_load.csv'))
               df['kwh'] = df['kwh'].apply(ast.literal_eval)
          df = df.rename(columns={'kwh':'consumption_hourly'})

          on = self._find_geography_column_to_merge_on(df)
          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=on)

     def load_interconnection_settings(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder, 'interconnection_limits.csv'), index_col=None)
          on = self._find_geography_column_to_merge_on(df)
          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=on)

     def load_normalized_hourly_resource_solar(self):
          if os.path.exists(os.path.join(self.input_csv_folder,'solar_resource_hourly.json')):
               df = pd.read_json(os.path.join(self.input_csv_folder,'solar_resource_hourly.json'))
          elif os.path.exists(os.path.join(self.input_csv_folder,'solar_resource_hourly.csv')):
               df = pd.read_csv(os.path.join(self.input_csv_folder,'solar_resource_hourly.csv'))
               df['cf'] = df['cf'].apply(ast.literal_eval)

          df = df.rename(columns={'cf':'solar_cf_profile'})

          on = self._find_geography_column_to_merge_on(df)
          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=on)

     def load_electric_rates_json(self):
          if os.path.exists(os.path.join(self.input_csv_folder,'urdb3_rates.json')):
               df = pd.read_json(os.path.join(self.input_csv_folder,'urdb3_rates.json'))
          elif os.path.exists(os.path.join(self.input_csv_folder,'urdb3_rates.csv')):
               df = pd.read_csv(os.path.join(self.input_csv_folder,'urdb3_rates.csv'))

          df.rename(columns={'rate_json':'tariff_dict', 'rate_id_alias':'tariff_id'}, inplace=True)
          df['tariff_dict'] = df['tariff_dict'].apply(lambda x: json.loads(x))
          
          on = self._find_geography_column_to_merge_on(df)
          self.core_agent_attributes = self.core_agent_attributes.merge(df, on=on)

     def load_max_market_share(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder,'max_market_share_settings.csv'), index_col=None)

          mms_filter = []
          for sector,settings  in list(self.sector_data.items()):
               mms_filter.append( list((df['sector_abbr'] == sector) & ( df['source'] == settings.max_market_curve_name ) & (df['business_model'] == 'host_owned')))
               mms_filter.append( list((df['sector_abbr'] == sector) & ( df['source'] == "NREL" ) & (df['business_model'] == 'tpo')))

          df = df[np.any(mms_filter,axis=0)]

          df_selection = df[(df['metric_value']==30) & (df['metric']=='payback_period') & (df['business_model']=='host_owned')]
          df_selection['metric_value'] = 30.1
          df_selection['max_market_share'] = 0
          self.market_share_parameters = pd.concat([df,df_selection], sort=False)

     def load_load_growth(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder,'load_growth_projections.csv'),index_col=None)
          df.rename(columns={'scenario':'load_growth_scenario'}, inplace=True)
          df = df.loc[df['load_growth_scenario'] == self.scenarios['load_growth_scenario_name']]

          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = df
          else:
               on = self._find_geography_column_to_merge_on(df)
               self.control_reg_trajectories = self.control_reg_trajectories.merge(df, on=on)


     def load_financing_rates(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder,'financing_rates.csv'), index_col=None)
          df = df.drop('social_indicator', axis='columns')
          on = self._find_geography_column_to_merge_on(df)
          self.core_agent_attributes = pd.merge(self.core_agent_attributes, df, on=on)

     def load_avoided_costs(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder,'avoided_cost_rates.csv'), encoding='utf-8-sig',index_col=None)
          on = self._find_geography_column_to_merge_on(df)
          df = df.melt(on[0],var_name='year', value_name='hourly_excess_sell_rate_usd_per_kwh')
          on.append('year')
          df['year'] = df['year'].astype(int)

          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = df
          else:
               self.control_reg_trajectories = self.control_reg_trajectories.merge(df, on=on)

     def load_wholesale_electricity(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder,'wholesale_rates.csv'),index_col=None)
          on = self._find_geography_column_to_merge_on(df)
          df = df.melt(on[0],var_name='year', value_name='wholesale_elec_usd_per_kwh')
          on.append('year')
          df['year'] = df['year'].astype(int)

          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = df
          else:
               self.control_reg_trajectories = self.control_reg_trajectories.merge(df, on=on)

     def load_rate_escalations(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder,'rate_escalations.csv'),index_col=None)
          df.rename(columns={'escalation_factor':'elec_price_multiplier'}, inplace=True)
          re_filter = []
          for sector,settings  in list(self.sector_data.items()):
               re_filter.append( list((df['sector_abbr'] == settings.sector_abbr.lower()) & ( df['source'] == settings.rate_escalation_name ) ))
          df = df[np.any(re_filter,axis=0)]

          if self.control_reg_trajectories.empty:
               self.control_reg_trajectories = df
          else:
               on = self._find_geography_column_to_merge_on(df)
               self.control_reg_trajectories = self.control_reg_trajectories.merge(df, on=on)

     def load_bass_params(self):
          df = pd.read_csv(os.path.join(self.input_csv_folder,'pv_bass.csv'),index_col=None)
          if 'teq_yr1' not in df.columns:
               df['teq_yr1'] = 2
          if 'tech' not in df.columns:
               df['tech'] = 'solar'

          on = self._find_geography_column_to_merge_on(df)

          if self.state_start_conditions.empty:
               self.state_start_conditions = df
          else:
               self.state_start_conditions = self.state_start_conditions.merge(df, on=on)

     def get_pv_specs(self):
          on = self._find_geography_column_to_merge_on(self.pv_trajectories)
          return self.pv_trajectories[on + ['pv_power_density_w_per_sqft','pv_deg','pv_price_per_kw','pv_om_per_kw','pv_variable_om_per_kw']]

     def get_batt_price_trajectories(self):
          on = self._find_geography_column_to_merge_on(self.storage_trajectories)
          return self.storage_trajectories[on+['batt_price_per_kwh','batt_price_per_kw','batt_om_per_kw','batt_om_per_kwh']]

     def get_financing_terms(self):
          on = self._find_geography_column_to_merge_on(self.financial_trajectories)
          return self.financial_trajectories[on+['deprec_sch','loan_term','itc_fraction','tax_rate','economic_lifetime']]

     def get_rate_escalations(self):
          on = self._find_geography_column_to_merge_on(self.control_reg_trajectories)
          return self.control_reg_trajectories[on+['elec_price_multiplier']]

     def get_wholesale_elec_prices(self):
          on = self._find_geography_column_to_merge_on(self.control_reg_trajectories)
          return self.control_reg_trajectories[on+['wholesale_elec_usd_per_kwh']]

     def get_load_growth(self,year):
          on = self._find_geography_column_to_merge_on(self.control_reg_trajectories)
          return self.control_reg_trajectories[self.control_reg_trajectories['year']==year][on+['load_multiplier']]

     def get_nem_settings(self,year):
          on = self._find_geography_column_to_merge_on(self.control_reg_trajectories)
          return self.control_reg_trajectories[self.control_reg_trajectories['year']==year][on+['nem_system_size_limit_kw','wholesale_elec_usd_per_kwh','hourly_excess_sell_rate_usd_per_kwh']]

     def get_max_market_share(self):
          return self.market_share_parameters

     def get_bass_params(self):
          on = self._find_geography_column_to_merge_on(self.state_start_conditions)
          return self.state_start_conditions[on+['p','q','teq_yr1','tech']]

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

    output_dir = datetime #str(input('Run name (default of formatted_time):')) or datetime

    model_settings.set('cdate', datetime)
    model_settings.set('out_dir',  '%s/runs/results_%s' % (os.path.dirname(os.getcwd()), output_dir))
    model_settings.set('git_hash', utilfunc.get_git_hash())
    
    # --- check for scenarios listed in config ---
    input_scenarios = [s for s in glob.glob(os.path.join(os.pardir,'input_scenarios','*.xls*')) if not '~$' in s]
    if SCENARIOS == None:
        pass
    else:
        input_scenarios = [s for s in input_scenarios if s.split('/')[-1].split('.xls')[0] in SCENARIOS]
    model_settings.set('input_scenarios', input_scenarios)

    #==========================================================================================================
    # validate model settings and make the ouput directory
    #==========================================================================================================
    model_settings.validate()
    os.makedirs(model_settings.out_dir)

    return model_settings

def init_scenario_settings(scenario_file, model_settings):
     """load scenario specific data and configure output settings"""

     scenario_settings = load_scenario_to_inputSheet(scenario_file, model_settings)
     scenario_settings.write_folders(model_settings)
     scenario_settings.write_inputs()
     scenario_settings.validate()
     return scenario_settings 
