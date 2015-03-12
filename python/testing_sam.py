# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 15:27:18 2015

@author: mgleason
"""
import numpy as np
import pandas as pd
import pssc_mp

def scale_array(row, array_col, scale_col, prec_offset_value):
    
    row[array_col] = (np.array(row[array_col]) * row[scale_col])/prec_offset_value
    
    return row    

def update_rate_json_w_nem_fields(row):
    
    nem_fields = ['ur_enable_net_metering', 'ur_nm_yearend_sell_rate', 'ur_flat_sell_rate']
    nem_dict = dict((k, row[k]) for k in nem_fields)
    row['rate_json'].update(nem_dict)
    
    return row

def get_inputs(con):
    
    inputs_dict = locals().copy()     
       
    inputs_dict['load_scale_offset'] = 1e8
    inputs_dict['gen_scale_offset'] = 1e6    
    
    sql = """WITH eplus as 
            (
            	SELECT hdf_index, crb_model, nkwh
            	FROM diffusion_shared.energy_plus_normalized_load_res
            	WHERE crb_model = 'reference'
            	UNION ALL
            	SELECT hdf_index, crb_model, nkwh
            	FROM diffusion_shared.energy_plus_normalized_load_com
            ), 
            a as
            (
            	select rate_id_alias, rate_source,
            				hdf_load_index, crb_model, load_kwh_per_customer_in_bin,
            				solar_re_9809_gid, tilt, azimuth, system_size_kw, 
            				ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate
            	from diffusion_solar.pt_com_best_option_each_year --- CHANGE THIS TO wes.
            	where county_id = 330 and bin_id = 7
            )
            SELECT 1 as uid, 
            	b.sam_json as rate_json, 
            	a.load_kwh_per_customer_in_bin, c.nkwh as consumption_hourly,
            	a.system_size_kw, d.cf as generation_hourly,
            	a.ur_enable_net_metering, a.ur_nm_yearend_sell_rate, a.ur_flat_sell_rate
            from a
            LEFT JOIN diffusion_solar.all_rate_jsons b 
                ON a.rate_id_alias = b.rate_id_alias
                AND a.rate_source = b.rate_source
            
            -- JOIN THE LOAD DATA
            LEFT JOIN eplus c
            	ON a.crb_model = c.crb_model
            	AND a.hdf_load_index = c.hdf_index
            
            -- JOIN THE RESOURCE DATA
            LEFT JOIN diffusion_solar.solar_resource_hourly d
            	ON a.solar_re_9809_gid = d.solar_re_9809_gid
            	AND a.tilt = d.tilt
            	AND a.azimuth = d.azimuth;"""
             
    df = pd.read_sql(sql, con, coerce_float = False)      

    #
    df = df.apply(scale_array, axis = 1, args = ('consumption_hourly','load_kwh_per_customer_in_bin', inputs_dict['load_scale_offset']))
    
    # scale the hourly cfs into hourly kw using the system size
    df = df.apply(scale_array, axis = 1, args = ('generation_hourly','system_size_kw', inputs_dict['gen_scale_offset']))
    
    # update the net metering fields in the rate_json
    df = df.apply(update_rate_json_w_nem_fields, axis = 1)
    
    return df[['uid','rate_json','consumption_hourly','generation_hourly']]
      

# add code to create connection
# [HERE] -- connect to dnpdb001.bigde.nrel.gov, use diffusion_3

# get the inputs for sam
rate_input_df = get_inputs(con)   
consumption_array = rate_input_df['consumption_hourly'][0]
# open a file
# dump this to file
consumption_array.tostring()
# close file


# run sam
sam_results_df = pssc_mp.pssc_mp(rate_input_df, cfg.local_cores)      

# inspect the results
  