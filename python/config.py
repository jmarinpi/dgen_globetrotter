# -*- coding: utf-8 -*-
"""
Edited Monday Nov 5, 218
@author: tkwasnik
"""
import os
import multiprocessing

#==============================================================================
# these are all variables that we can change, but don't want to expose to non-expert users
#==============================================================================

SECTORS = ['res','com','ind']
SECTOR_NAMES = {'res':'Residential','com':'Commercial','ind':'Industrial'}
TECHS = [['solar']]
TECH_MODES = ['elec']

#==============================================================================
#   get the path of the current file
#==============================================================================
MODEL_PATH = os.path.dirname(os.path.abspath(__file__))

#==============================================================================
#   model start year
#==============================================================================
START_YEAR = 2016

#==============================================================================
#   local cores
#==============================================================================
LOCAL_CORES = multiprocessing.cpu_count()
