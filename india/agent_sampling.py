import pandas as pd
import numpy as np
import os
import helper
import geopandas as gpd
import agent_config as config


pd.options.mode.chained_assignment = None


# """
# --- agent_core_attributes_all.csv ---

# Columns
# -------

#     state (str) : Usually the sub-federal political geography, sometimes a sub-sub feder (i.e. county)
#     state_id (int) : integer representation of state
#     sector_abbr (str) : the sector of the agent
#     tariff_class (str) : the tariff class (particularly relevant in countries with crosssubsidization)
#     customers_in_bin (int) : customers represented by agent
#     load_in_bin_kwh (int) : annual kWh represented by agent
#     load_per_customer_in_bin_kwh (int) : load_in_bin_kwh / customers_in_bin
#     developable_roof_sqft (int) : total installable rooftop area TODO move this to a seperate csv? 
# """


#%%
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~ Load Files ~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# --- Load Required Files ---
def load_con():
    """ Load csv with India consumption. """
    # total consumption by geography/sector
    con = pd.read_csv(os.path.join('reference_data', 'discom_consumption.csv'))
    assert config.GEOGRAPHY in con.columns
    
    # --- Group Consumption by State ---
    for sector in ['residential','commercial','irrigation','industrial']:
        con[f'{sector}_gwh'] = con['consumption_gwh_2016'] * con[f'con_{sector}_pct']
        
    con.loc[con['state_name'] == 'telangana', 'state_name'] = 'andhra_pradesh' #census data is from 2011, before Telangana was a state! change after 2021 census. 
    
    agg_funcs = {
        'residential_gwh':'sum',
        'commercial_gwh':'sum',
        'irrigation_gwh':'sum',
        'industrial_gwh':'sum',
        'consumption_gwh_2016':'sum', 
        'forecast_consumption_gwh_2026':'sum',
        'annual_growth_rate':'mean',
        'losses_2016':'mean',
        'forecast_losses_2026':'mean'
        }
    
    con = con.groupby(config.GEOGRAPHY, as_index=False).agg(agg_funcs)
    
    return con

def load_census():
    # population by district
    census = pd.read_csv(os.path.join('reference_data','india_census','india_district_pop_2011.csv'))
    return census

#%%

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~ Make Distributions ~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# --- Assign Distribution Parameters (from SS19 as fill in if India data not available) ---
    
# SQL statement to get weighted average of developable roof sqft (U.S.) by sector
# SELECT year, sector_abbr, ROUND(SUM(developable_roof_sqft * customers_in_bin) / SUM(customers_in_bin),1) as weighted_avg
# FROM "10_ss20_mid_case".agent_outputs
# WHERE year = 2020
# GROUP BY year, sector_abbr
# ORDER BY weighted_avg, year
developable_sqft_mu = {'res':385, 'com':6378, 'ind':5329, 'agg': 20000} #20000 sqft ~ 0.5 acre for agg 

# SELECT year, sector_abbr, ROUND(SQRT(SUM(customers_in_bin*POWER(developable_roof_sqft,2))/SUM(customers_in_bin) - POWER(SUM(customers_in_bin*developable_roof_sqft)/SUM(customers_in_bin), 2)),0) as weighted_std
# FROM "10_ss20_mid_case".agent_outputs
# WHERE year = 2020
# GROUP BY year, sector_abbr
# ORDER BY weighted_std, year					  
developable_sqft_sigma = {'res':363, 'com':12526, 'agg': 20000, 'ind':13758} # SS19 std developable sqft by sector\

# WITH std_table AS (
# 	SELECT year, sector_abbr,
# 		SQRT(SUM(customers_in_bin*POWER(CAST(load_kwh_per_customer_in_bin as float),2))/SUM(customers_in_bin) - POWER(SUM(customers_in_bin*CAST(load_kwh_per_customer_in_bin as float))/SUM(customers_in_bin), 2)) as weighted_std
# 	FROM "10_ss20_mid_case".agent_outputs
# 	WHERE year = 2020
# 	GROUP BY year, sector_abbr),
# 	
# 	mean_table AS(
# 	SELECT year, sector_abbr,
# 		SUM(CAST(load_kwh_per_customer_in_bin as float) * customers_in_bin) / SUM(customers_in_bin) as weighted_avg
#  	FROM "10_ss20_mid_case".agent_outputs
# 	WHERE year = 2020
# 	GROUP BY year, sector_abbr)

# SELECT s.sector_abbr, s.weighted_std / m.weighted_avg as pct
# FROM std_table as s
# 	INNER JOIN mean_table as m
# 	ON s.year = m.year AND s.sector_abbr = m.sector_abbr
# all_geo_sigma_load = {'res':0.58, 'com':2.5, 'agg':0, 'ind':6.10} # SS19 std load by sector / SS19 avg load by sector
all_geo_sigma_load = {'res':0.1, 'com':0.1, 'agg':0.1, 'ind':0.1} #TODO: the SS19 values make no sense. need to visualize them to understand if a normal distribution is the right way to model this

# SELECT sector_abbr, year, SUM(customers_in_bin)
# FROM "10_ss20_mid_case".agent_outputs
# WHERE year = 2020
# GROUP BY year, sector_abbr
customers_per_hh_by_sector = {'res':1, 'com':0.07, 'agg':0.01, 'ind':0.01} # SS19 customers_in_bin sum by sector / SS19 residential customers_in_bin

# --- Functions to make distributions ---

def make_load_count(con):
    """ dict of kwh by geography/sector. """
    sector_gwh_columns = ['residential_gwh','commercial_gwh','irrigation_gwh','industrial_gwh']
    load_count = con[sector_gwh_columns]
    load_count *= 1000000 #gwh to kwh
    load_count = load_count.round(0)
    load_count.columns = ['res','com','agg','ind'] 
    load_count.index = con[config.GEOGRAPHY]
    load_count = load_count.to_dict('index')
    return load_count

def make_hh_count(census):
    """ dict of n households by geography. """
    district_cols = [config.GEOGRAPHY, 'district_name', 'households']
    hh_count = census[district_cols]
    hh_count = hh_count.groupby(config.GEOGRAPHY)['households'].sum()
    hh_count = hh_count.to_dict()
    return hh_count

def make_sector_dist_load(con):
    """ dict of pct of customers by load by sector in geography. """
    sector_gwh_columns = ['residential_gwh','commercial_gwh','irrigation_gwh','industrial_gwh']
    sector_dist = con[sector_gwh_columns]
    for c in sector_gwh_columns: #convert to percents normalized to 1
        sector_dist[c] = sector_dist[c] / con[sector_gwh_columns].sum(axis=1)
    sector_dist.columns = ['res','com','agg','ind'] 
    sector_dist.index = con[config.GEOGRAPHY]
    sector_dist = sector_dist.to_dict('index')
    return sector_dist

def make_agent_count(sector_dist):
    """ dict of agents by sector in geography. """
    agent_count = {}
    for geo, d in sector_dist.items():
        agent_count[geo] = {}
        for sector, pct in d.items():
            agent_count[geo][sector] = round(config.AGENTS_PER_GEOGRAPHY * sector_dist[geo][sector])
    return agent_count

def make_district_dist(census, agent_count):
    """ dict of district hh by geography/district. """
    district_cols = [config.GEOGRAPHY, 'district_name', 'households']
    district_pct = census[district_cols]
    district_pct['total_hh'] = district_pct.groupby(config.GEOGRAPHY)['households'].transform('sum')
    district_pct['hh_pct'] = district_pct['households'] / district_pct['total_hh']
    district_pct = district_pct.pivot(index=config.GEOGRAPHY, columns='district_name', values='hh_pct')
    district_pct = district_pct.to_dict('index')
    for geo, d in district_pct.items(): #get rid of nans
        district_pct[geo] = {k:v for k,v in d.items() if v > 0}
    district_dist = {}
    for geo in census[config.GEOGRAPHY].unique():
        district_dist[geo] = {}
        for sector in ['res','com','agg','ind']:
            n_agents = agent_count[geo][sector]
            districts = []
            for i in range(n_agents): #for every agent in this geo/sector
                dice_roll = np.random.uniform(0,1,1)[0]
                count = 0
                for district, pct in district_pct[geo].items():
                    count += pct
                    if count > dice_roll: #by picking random float (0-1) and cumulatively adding together percentages of household in districts within geography until percentage is greater than the random number
                        districts.append(district)
                        break # break out of dictrict, pct forloop 
            district_dist[geo][sector] = districts
    return district_dist

def make_roof_dist(census, agent_count, developable_sqft_mu, developable_sqft_sigma):
    """ dict of developable_roof_sqft by geography/sector based on normal distribution. """
    developable_sqft_dist = {}
    for geo in census[config.GEOGRAPHY].unique():
        developable_sqft_dist[geo] = {}
        for sector in ['res','com','agg','ind']:
            n_agents = agent_count[geo][sector]
            dist = np.random.normal(loc=developable_sqft_mu[sector], #create distribution of rootop sizes for the geography/sector
                                    scale=developable_sqft_sigma[sector],
                                    size=n_agents)
            lower_bound = developable_sqft_mu[sector] * 0.1
            dist = np.clip(dist, lower_bound, None) #clip lower bound of developable roof sqft at 10 percent of average for sector
            developable_sqft_dist[geo][sector] = dist
    return developable_sqft_dist
        
def make_load_dist(census, agent_count, hh_count, load_count,
                   customers_per_hh_by_sector, all_geo_sigma_load):
    """ dict of load_per_customer_in_bin_kwh by geography/sector. """
    customers_in_bin_dist = {}
    load_per_customer_in_bin_dist = {}
    for geo in census[config.GEOGRAPHY].unique():
        load_per_customer_in_bin_dist[geo] = {}
        customers_in_bin_dist[geo] = {}
        for sector in ['res','com','agg','ind']:
            n_agents = agent_count[geo][sector] #get n_agents in sector and geography as int
            if n_agents > 0:
                n_hh = hh_count[geo]
                n_customers = round(customers_per_hh_by_sector[sector] * n_hh)
                n_load = load_count[geo][sector]
                mu_load = n_load / n_customers #average annual kwh per customer across entire distribution
                sigma_load = all_geo_sigma_load[sector] * mu_load #std of annual kwh per customer across entire distribution
                dist = np.random.normal(mu_load, sigma_load, size=n_customers) # create distribution
                count_hh, load_bins = np.histogram(dist, bins=n_agents) #create histogram with number of agents as bins
                load_bins = [(sum([load_bins[i], load_bins[i+1]]) / 2) for i in range(len(load_bins)-1)] #center bins as mean between edges
                load_dist = np.clip(load_bins, 0, None) #clip lower bound of developable roof sqft at 10 percent of average for sector
                customers_in_bin_dist[geo][sector] = count_hh
                load_per_customer_in_bin_dist[geo][sector] =  load_dist
            else:
                customers_in_bin_dist[geo][sector] = 0
                load_per_customer_in_bin_dist[geo][sector] = 0
    return customers_in_bin_dist, load_per_customer_in_bin_dist
       

#%%

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~ Create Agents ~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
def initialize_agents(agent_count):      
    """ Initialize agents based on dict of agent_count. with sector and geography columns. """
    agents = pd.DataFrame()

    # - create rows of agents with sector and geography
    for geo, d in agent_count.items(): #consistent number of agents between geographies, with distribution by load in sector
        for sector, n_agents in d.items():
            for n in range(n_agents):
                agent = pd.DataFrame({config.GEOGRAPHY:[geo], 'sector_abbr':[sector], 'geo_sector_n_agents':n_agents})
                agents = agents.append(agent)
    agents.reset_index(drop=True, inplace=True)  
    return agents

def assign_distribution(agents, distribution, col_name):
    # - proportionally assign district 
    for geo in agents[config.GEOGRAPHY].unique():
        for sector in ['res','com','agg','ind']:
            agents.loc[(agents[config.GEOGRAPHY] == geo) & (agents['sector_abbr'] == sector), col_name] = distribution[geo][sector]
    return agents

def clean_agents(agents):
    agents = agents.loc[(agents['customers_in_bin'] > 0) & (agents['load_per_customer_in_bin_kwh'] > 0)] # drop agents without load or customers
    agents['load_in_bin_kwh'] = agents['customers_in_bin'] * agents['load_per_customer_in_bin_kwh']
    return agents
    

def test_load(agents, load_count):
    """ Test that load in agents equals load in load_count dict between geography/sectors. """
    for geo in agents[config.GEOGRAPHY].unique():
        for sector in ['res','com','agg','ind']:
            load_con = load_count[geo][sector]
            load_df = agents.loc[(agents['sector_abbr'] == sector) & (agents[config.GEOGRAPHY] == geo)]
            load_agents = (load_df['load_per_customer_in_bin_kwh'] * load_df['customers_in_bin']).sum()
            diff_pct = (load_agents - load_con) / load_con
            if load_con > 0:
                assert abs(diff_pct) < 0.02


def plot_normal_distribution():
    import numpy as np
    import matplotlib.pyplot as plt
    
    dist = np.random.normal(mu_load, sigma_load, size=n_customers)
    
    fig, ax = plt.subplots()
    count_hh, bins, ignored = ax.hist(dist, n_agents) #construct histogram with bins equal to n agents
    ax.set_xlabel('load')
    ax.set_ylabel('customers in bin')
    
def map_geo_ids(agents):
    state_id_lookup = pd.read_csv(os.path.join('reference_data', 'india_census','state_id_lookup.csv'))
    state_id_lookup = dict(zip(state_id_lookup['state_name'],state_id_lookup['state_id']))
    district_id_lookup = pd.read_csv(os.path.join('reference_data','india_census','district_id_lookup.csv'))
    district_id_lookup = dict(zip(district_id_lookup['district_name'], district_id_lookup['district_id']))
    agents['state_id'] = agents['state_name'].map(state_id_lookup)
    agents['district_id'] = agents['district_name'].map(district_id_lookup)
    return agents

def map_hdi(agents):
    hdi = pd.read_csv(os.path.join('reference_data','india_UN_HDI.csv'))[['Region','2018']]
    hdi.columns = ['state_name', 'social_indicator']
    hdi['state_name'] = hdi['state_name'].apply(helper.sanitize_string)
    agents = agents.merge(hdi)
    return agents
    

def merge_district_geometry(agents):
    # --- load district shapefile ---
    districts = gpd.read_file(os.path.join('reference_data','districts_shapefile', 'India_Districts_ADM2_GADM.shp'))
    districts = districts[['NAME_1','NAME_2','VARNAME_2','geometry']]
    districts.columns = ['state_name','district_name','var_district_name','geometry']
    
    # --- clean state and district columns ---
    districts['state_name'] = districts['state_name'].apply(helper.sanitize_string)
    districts['district_name'] = districts['district_name'].apply(helper.sanitize_string)
    
    # --- create fuzzy list for string matching
    districts['fuzzy_str'] = districts['state_name'] + '_' + districts['district_name']
    
    # --- create clean list for string matchibng ---
    agents['clean_str'] = agents['state_name'] + '_' + agents['district_name']
    
    from cfuzzyset import cFuzzySet as FuzzySet
    def fuzzy_address_matcher(fuzzy_list, clean_list, thresh=0.5):
    
        if isinstance(fuzzy_list, pd.Series):
            fuzzy_list = fuzzy_list.tolist()
        
        if isinstance(clean_list, pd.Series):
            clean_list = clean_list.unique().tolist()
    
        index = FuzzySet()
        
        for c in clean_list:
            index.add(c)
        
        out_list = []
        for f in fuzzy_list:
            result = index.get(f)
            out_list.append(result[0][1])
        
        return out_list
    
    # --- fuzzy string matching ---
    districts['clean_str'] = fuzzy_address_matcher(districts['fuzzy_str'], agents['clean_str'])
    
    # --- merge geometry onto agents ---
    agents = agents.merge(districts[['clean_str','geometry']], on='clean_str')
    agents.drop(['clean_str'], axis='columns')
    
    # --- find centroid for each agent ---
    agents['centroid'] = [i.centroid for i in agents['geometry']]
    agents.drop(['geometry'], axis='columns')
    
    return agents

def map_tariff_ids(agents):
    """
    Map tariff details such as tiers, prices, demand charges, and other info. Not used much yet. 
    """
    
    agents['tariff_id'] = agents['sector_abbr'] + '#' + agents[config.GEOGRAPHY]
    return agents

    





