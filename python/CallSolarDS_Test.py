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

# Solve year most recenlty completed in ReEDS
year = sys.argv[1]

# Path to current ReEDS run
reeds_path = sys.argv[2]
#reeds_path = "C:/ReEDS/OtherReEDSProject/inout"

#Path to GAMS
gams_path = sys.argv[3]

#Path to the gdx files that hold the SolarDS inputs
gdxfile_in = reeds_path + "/gdxfiles/SolarDS_Input_%s.gdx" % year

#Pull the SolarDS inputs from ReEDS
ReEDS_df = gdxpds.to_dataframes(gdxfile_in)

#Change working directory to where SolarDS is
os.chdir('C:/ReEDS/SolarDS/python')

# Run SolarDS
df = dgen_model.main(mode = 'ReEDS', resume_year = year, ReEDS_inputs = ReEDS_df)
df = df[(df['year'] == year)]
SolarDSPVcapacity = 0.001* df.groupby('pca_reg')['installed_capacity'].sum() # Convert output from kW to MW and sum to the PCA level
SolarDSPVcapacity = SolarDSPVcapacity.to_dict()

#Use old SolarDS outputs until things are running
#csvPath = reeds_path + "/includes/SunShot62.5.csv"
#SolarDSPVcapacity_all = pd.read_csv(csvPath)
#SolarDSPVcapacity = SolarDSPVcapacity_all[['BA', year]]
SolarDSPVcapacity = SolarDSPVcapacity.rename(columns = {year:'value'})
data = {'SolarDSPVcapacity': SolarDSPVcapacity}

gdxfile_out = reeds_path + "/gdxfiles/SolarDS_Output_%s.gdx" % year

gdx = gdxpds.to_gdx(data, gdxfile_out)
