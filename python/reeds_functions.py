# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 10:54:39 2015

@author: mgleason
"""
import pickle
import pandas as pd


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


def summarise_solar_resource_by_ts_and_pca_reg(df, con):
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
    
    # Query the solar resource by pca, tilt, azimuth, & timeslice and rename columns e.g. at this point resource has already been averaged over solar_re_9809_gid 
    resource = pd.read_sql("SELECT * FROM diffusion_solar.solar_resource_by_pca_summary;", con)
    resource.drop('npoints', axis =1, inplace = True)
    resource['pca_reg'] = 'p' + resource.pca_reg.map(str)
    resource.columns = ['pca_reg','tilt','azimuth','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17']

    # Determine the percentage of adopters that have selected a given azimuth/tilt combination in the pca
    d = df[['installed_capacity','pca_reg', 'azimuth', 'tilt']].groupby(['pca_reg', 'azimuth', 'tilt']).sum()    
    d = d.groupby(level=0).apply(lambda x: x/float(x.sum())).reset_index()
    
    # Join the resource to get the capacity factor by time slice, azimuth, tilt & pca
    d = pd.merge(d, resource, how = 'left', on = ['pca_reg','azimuth','tilt'])
    
    # Pivot to tall format
    d = d.set_index(['pca_reg','azimuth','tilt','installed_capacity']).stack().reset_index()
    d.columns = ['pca_reg','azimuth','tilt','installed_capacity','ts','cf']
    
    # Finally, calculate weighted mean CF by timeslice using number_of_adopters as the weight 
    d['cf'] = d['cf'] * d['installed_capacity']
    d = d.groupby(['pca_reg','ts']).sum().reset_index()
    d.drop(['tilt','installed_capacity'], axis = 1, inplace = True)
    
    return d