# -*- coding: utf-8 -*-
"""
This module contains variables that can be changed, but are not exposed to non-expert users.
"""
import os
import multiprocessing

#==============================================================================

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
LOCAL_CORES = int(multiprocessing.cpu_count() * 0.4)
# LOCAL_CORES = 1

#==============================================================================
#   silence some output
#==============================================================================
VERBOSE = False