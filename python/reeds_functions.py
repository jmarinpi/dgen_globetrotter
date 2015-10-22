# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 10:54:39 2015

@author: mgleason
"""
import pickle
import pandas as pd
import os


def load_resume_vars(cfg, resume_year):
    # Load the variables necessary to resume the model
    if resume_year == 2014:
        cfg.init_model = True
        out_dir = None
        input_scenarios = None
        market_last_year = None
    else:
        cfg.init_model = False
        # Load files here
        market_last_year = pd.read_pickle("market_last_year.pkl")   
        with open('saved_vars.pickle', 'rb') as handle:
            saved_vars = pickle.load(handle)
        out_dir = saved_vars['out_dir']
        input_scenarios = saved_vars['input_scenarios']
    return cfg.init_model, out_dir, input_scenarios, market_last_year


def combine_outputs_reeds(schema, sectors, cur, con, year):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()   

    sql = '''DROP TABLE IF EXISTS %(schema)s.reeds_outputs;
            CREATE UNLOGGED TABLE %(schema)s.reeds_outputs AS  ''' % inputs  
    
    for i, sector_abbr in enumerate(sectors.keys()):
        inputs['sector'] = sectors[sector_abbr].lower()
        inputs['sector_abbr'] = sector_abbr
        if i > 0:
            inputs['union'] = 'UNION ALL '
        else:
            inputs['union'] = ''
        
        sub_sql = '''%(union)s 
                    SELECT '%(sector)s'::text as sector, 

                    a.micro_id, a.county_id, a.bin_id, a.year, a.new_capacity, a.installed_capacity, 
                    b.azimuth,b.tilt,b.customers_in_bin,
                    b.state_abbr, b.pca_reg, b.reeds_reg,
                    (b.rate_escalation_factor * a.first_year_bill_without_system)/b.load_kwh_per_customer_in_bin as cost_of_elec_dols_per_kwh,
                    a.excess_generation_percent
                                        
                    FROM %(schema)s.outputs_%(sector_abbr)s a
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                    and a.year = b.year
                    WHERE a.tech = 'solar'
                    AND a.year = %(year)s

                    ''' % inputs
        sql += sub_sql
    sql += ';'
    cur.execute(sql)
    con.commit()
    sql2 = 'SELECT * FROM %(schema)s.reeds_outputs' % inputs
    return pd.read_sql(sql2,con)


def summarise_solar_resource_by_ts_and_pca_reg(schema, con):
    '''
    Outputs for ReEDS linkage the solar capacity factor by time slice and PCA 
    weighted by the existing azimuth/tilts deployed. Summary is based
    on a pre-processing step which finds the mean CF by timeslice by averaging
    over the point-level resource (solar_re_9808_gid) within a PCA
    
    IN: 
        con
        df
    
    OUT:
        
        pandas dataframe [pca_reg, ts, cf]
    '''
    
    inputs = locals().copy()    
    
    # Query the solar resource by pca, tilt, azimuth, & timeslice and rename columns e.g. at this point resource has already been averaged over solar_re_9809_gid 
    sql = """WITH a AS
            (
                	SELECT pca_reg, azimuth, tilt, year, sum(installed_capacity) as installed_capacity
                	FROM %(schema)s.reeds_outputs a
			WHERE installed_capacity > 0
                	GROUP BY pca_reg, azimuth, tilt, year
            ),
            b AS
            (
                	SELECT pca_reg, year, sum(installed_capacity) as installed_capacity
                	FROM %(schema)s.reeds_outputs a
			WHERE installed_capacity > 0
                	GROUP BY pca_reg, year
            ),
            c AS
            (
                	SELECT a.pca_reg, a.azimuth, a.tilt, a.year,
                		a.installed_capacity/b.installed_capacity as pct_installed_capacity,
                		c.reeds_time_slice as ts,
                		c.cf_avg as cf
                	FROM a
                	LEFT JOIN b
                    	ON a.pca_reg = b.pca_reg
                    	AND a.year = b.year
                	LEFT JOIN diffusion_solar.reeds_solar_resource_by_pca_summary_tidy c
                     ON a.pca_reg = c.pca_reg
                     AND a.azimuth = c.azimuth
                     AND a.tilt = c.tilt
            )
            SELECT c.pca_reg, c.year, c.ts,
                	SUM(c.pct_installed_capacity * c.cf)/SUM(c.pct_installed_capacity) AS cf
            FROM c
            GROUP BY c.pca_reg, c.year, c.ts;""" % inputs
    # read to data frame
    cf_by_time_slice_pca_and_year = pd.read_sql(sql, con)

    return cf_by_time_slice_pca_and_year
    
    
def write_reeds_offline_mode_data(schema, con, out_scen_path, file_suffix = ''):
   
    inputs = locals().copy()   
   
   # Installed capacity and average electricity cost by pca and year
    sql = '''SELECT year, pca_reg, 
                     SUM(installed_capacity)/1000 as installed_capacity_mw, 
                     SUM(number_of_adopters * cost_of_elec_dols_per_kwh)/SUM(number_of_adopters) as cost_of_elec_dols_per_kwh
             FROM %(schema)s.outputs_all_solar
             WHERE number_of_adopters > 0
             GROUP BY year, pca_reg
             ORDER BY year, pca_reg;''' % inputs
    # read to data frame 
    installed_capacity_and_elec_cost_pca_year = pd.read_sql(sql, con)
    # write to csv
    installed_capacity_and_elec_cost_pca_year.to_csv(os.path.join(out_scen_path,'installed_capacity_and_elec_cost_pca_year%s.csv' % file_suffix), index = False)
    
    
    # calculate the weighted average capacity factor by pca_reg, year, and time slice
    # (weighted by the percent of installed capacity with each orientation)
    sql = """WITH a AS
            (
                	SELECT pca_reg, azimuth, tilt, year, sum(installed_capacity) as installed_capacity
                	FROM %(schema)s.outputs_all_solar a
			WHERE installed_capacity > 0
                	GROUP BY pca_reg, azimuth, tilt, year
            ),
            b AS
            (
                	SELECT pca_reg, year, sum(installed_capacity) as installed_capacity
                	FROM %(schema)s.outputs_all_solar a
			WHERE installed_capacity > 0
                	GROUP BY pca_reg, year
            ),
            c AS
            (
                	SELECT a.pca_reg, a.azimuth, a.tilt, a.year,
                		a.installed_capacity/b.installed_capacity as pct_installed_capacity,
                		c.reeds_time_slice as ts,
                		c.cf_avg as cf
                	FROM a
                	LEFT JOIN b
                    	ON a.pca_reg = b.pca_reg
                    	AND a.year = b.year
                	LEFT JOIN diffusion_solar.reeds_solar_resource_by_pca_summary_tidy c
                     ON a.pca_reg = c.pca_reg
                     AND a.azimuth = c.azimuth
                     AND a.tilt = c.tilt
            )
            SELECT c.pca_reg, c.year, c.ts,
                	SUM(c.pct_installed_capacity * c.cf)/SUM(c.pct_installed_capacity) AS cf
            FROM c
            GROUP BY c.pca_reg, c.year, c.ts;""" % inputs
    # read to data frame
    cf_by_time_slice_pca_and_year = pd.read_sql(sql, con)
    con.rollback()
    # write to csv
    cf_by_time_slice_pca_and_year.to_csv(os.path.join(out_scen_path,'cf_by_time_slice_pca_and_year%s.csv' % file_suffix), index = False)
    