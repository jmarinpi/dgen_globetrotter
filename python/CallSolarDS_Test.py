# -*- coding: utf-8 -*-
"""
Created on Fri Nov 07 11:26:31 2014

@author: wcole
"""
import sys
import gdxpds
import pandas as pd
import dgen_model
import os

def main(year, reeds_path, gams_path):

    # Path to the gdx files that hold the SolarDS inputs
    gdxfile_in = reeds_path + "/gdxfiles/SolarDS_Input_%s.gdx" % year
    
    # Pull the SolarDS inputs from ReEDS
    ReEDS_df = gdxpds.to_dataframes(gdxfile_in)
    
    # Change working directory to where SolarDS is
    os.chdir('../SolarDS/python')
    
    # Run SolarDS
    df = dgen_model.main(mode = 'ReEDS', resume_year = year, ReEDS_inputs = ReEDS_df)
    df = df[(df['year'] == year)]
    df.to_csv("temp.csv")
    SolarDSPVcapacity = 0.001* df.groupby('pca_reg')['installed_capacity'].sum() # Convert output from kW to MW and sum to the PCA level   
    SolarDSPVcapacity = SolarDSPVcapacity.reset_index()    
    
    # The data column has to be named "value" in order for gdxpds to work properly
    SolarDSPVcapacity = SolarDSPVcapacity.rename(columns = {'installed_capacity':'value'})
    data = {'SolarDSPVcapacity': SolarDSPVcapacity}
    
    gdxfile_out = reeds_path + "/gdxfiles/SolarDS_Output_%s.gdx" % year
    
    gdx = gdxpds.to_gdx(data, gdxfile_out)

if __name__ == '__main__':
    # Solve year most recenlty completed in ReEDS
    year = sys.argv[1]
    year = int(year)
    
    # Path to current ReEDS run
    reeds_path = sys.argv[2]
    #reeds_path = "C:/ReEDS/OtherReEDSProject/inout"
    
    # Path to GAMS
    gams_path = sys.argv[3]    
    main(year, reeds_path, gams_path)