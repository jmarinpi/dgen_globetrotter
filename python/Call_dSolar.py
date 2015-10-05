# -*- coding: utf-8 -*-
"""
Created on Fri Nov 07 11:26:31 2014

@author: wcole
"""
import sys
import gdxpds
import pandas as pd
import os

def main(year, endyr_ReEDS, reeds_path, gams_path, curtailment_method):
    # Path to the gdx files that hold the dSolar inputs
    gdxfile_in = reeds_path + "/gdxfiles/dSolar_Input_%s.gdx" % year
    
    # Pull the dSolar inputs from ReEDS
    ReEDS_df = gdxpds.to_dataframes(gdxfile_in, gams_path)
    
    # Change working directory to where dSolar is (must be done before importing dgen_model)
    os.chdir('../SolarDS/python')
    # os.chdir('c:/reeds/solards/python')

    import dgen_model
    reload(dgen_model)
    
    # Run dSolar
    ReEDS_inputs = {'ReEDS_df': ReEDS_df, 'curtailment_method':curtailment_method}
    df, cf_by_pca_and_ts = dgen_model.main(mode = 'ReEDS', resume_year = year, endyear = endyr_ReEDS, ReEDS_inputs = ReEDS_inputs)

    dSolarPVcapacity = 0.001* df.groupby('pca_reg')['installed_capacity'].sum() # Convert output from kW to MW and sum to the PCA level   
    dSolarPVcapacity = dSolarPVcapacity.reset_index()
    
    # Calculate mean retail rate weighted by population
    df['prod'] = df['customers_in_bin'] * df['cost_of_elec_dols_per_kwh']
    grouped = df[['pca_reg', 'cost_of_elec_dols_per_kwh', 'customers_in_bin','prod']].groupby('pca_reg').sum() 
    grouped['mean_retail_rate'] = grouped['prod']/grouped['customers_in_bin']
    mean_retail_rate = grouped.reset_index().drop(['cost_of_elec_dols_per_kwh', 'customers_in_bin','prod'], axis =1)
    
    # The data column has to be named "value" in order for gdxpds to work properly
    dSolarPVcapacity = dSolarPVcapacity.rename(columns = {'installed_capacity':'value'})
    cf_by_pca_and_ts = cf_by_pca_and_ts.rename(columns = {'cf':'value'})
    mean_retail_rate = mean_retail_rate.rename(columns = {'mean_retail_rate':'value'})
    
    data = {'dSolarPVcapacity': dSolarPVcapacity,
            'cf_by_pca_and_ts': cf_by_pca_and_ts,
            'mean_retail_rate': mean_retail_rate}
    
    gdxfile_out = reeds_path + "/gdxfiles/dSolar_Output_%s.gdx" % year
    
    gdx = gdxpds.to_gdx(data, gdxfile_out, gams_path)

if __name__ == '__main__':
    # Solve year most recently completed in ReEDS
    year = sys.argv[1]
    year = int(year)
    #year = 2014
    
    endyr_ReEDS = sys.argv[2]
    endyr_ReEDS = int(endyr_ReEDS)
    #endyr_ReEDS = 2050
    
    # Path to current ReEDS run
    reeds_path = sys.argv[3]
    #reeds_path = "C:/ReEDS/OtherReEDSProject/inout"
    
    # Path to GAMS
    gams_path = sys.argv[4]
    # gams_path = "C:/Gams/win64/24.1/bin"

	# Curtailment switch
    curtailment_method = sys.argv[5]
    curtailment_method = curtailment_method.lower() # Make sure it's on lower case to prevent errors later on.
    # curtailment_method = "off"
 
    main(year, endyr_ReEDS, reeds_path, gams_path, curtailment_method)