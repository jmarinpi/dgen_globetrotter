# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 10:51:51 2015

@author: mgleason
"""

import json
import getopt
import sys
import colorama
import logging
import colorlog
import pandas as pd

def get_pg_params(json_file):
    
    pg_params_json = file(json_file,'r')
    pg_params = json.load(pg_params_json)
    pg_params_json.close()

    pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s port=%(port)s' % pg_params

    return pg_params, pg_conn_string


def parse_command_args(argv):
    ''' Function to parse the command line arguments
    IN:
    
    -h : help 'dg_model.py -i <Initiate Model?> -y <year>'
    -i : Initiate model for 2010 and quit
    -y: or year= : Resume model solve in passed year
    
    OUT:
    
    init_model - Boolean - Should model initiate?
    resume_year - Float - year model should resume
    '''
    
    resume_year = None
    init_model = False
    
    try:
        opts, args = getopt.getopt(argv,"hiy:",["year="])
    except getopt.GetoptError:
        print 'Command line argument not recognized, please use: dg_model.py -i -y <year>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'dg_model.py -i <Initiate Model?> -y <year>'
            sys.exit()
        elif opt in ("-i"):
            init_model = True
        elif opt in ("-y", "year="):
            resume_year = arg
    return init_model, resume_year 


def init_log(log_file_path):
    
    colorama.init()
    logging.basicConfig(filename = log_file_path, filemode = 'w', format='%(levelname)-8s:%(message)s', level = logging.DEBUG)   
    logger = logging.getLogger(__name__)
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)-8s:%(reset)s %(white)s%(message)s",
        datefmt=None,
        reset=True
        )     
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    return logger


def shutdown_log(logger):
    logging.shutdown()
    for handler in logger.handlers:
        handler.flush()
        handler.close()
        logger.removeHandler(handler)
        
        
def code_profiler(out_dir):
    lines = [ line for line in open(out_dir + '/dg_model.log') if 'took:' in line]
    
    process = [line.split('took:')[-2] for line in lines]
    process = [line.split(':')[-1] for line in process]
    
    time = [line.split('took:')[-1] for line in lines]
    time = [line.split('s')[0] for line in time]
    time = [float(x) for x in time]
    
    
    profile = pd.DataFrame({'process': process, 'time':time})
    profile = profile.sort('time', ascending = False)
    profile.to_csv(out_dir + '/code_profiler.csv') 