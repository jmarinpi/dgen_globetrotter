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
import datetime
import select
import time
import subprocess
import os

#==============================================================================
#       Logging Functions
#==============================================================================


def get_logger(log_file_path=None):

    colorama.init()
    formatter = colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s:%(reset)s %(white)s%(message)s",
                                          datefmt=None,
                                          reset=True
                                          )
    if log_file_path is not None:
        logging.basicConfig(filename=log_file_path, filemode='w',
                            format='%(levelname)-8s:%(message)s', level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    if len(logger.handlers) == 0:
        logger = logging.getLogger(__name__)
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.setFormatter(formatter)
        logger.addHandler(console)

    return logger


def get_git_hash():

    try:
        fp = os.path.dirname(os.path.abspath(__file__))
        cmd = "cd %s;" % fp + "git log --pretty=format:'%h' -n 1"
        git_hash = subprocess.check_output(cmd, shell=True)
    except Exception, e:
        print e
        git_hash = 'unknown'

    return git_hash


def shutdown_log(logger):
    logging.shutdown()
    for handler in logger.handlers:
        handler.flush()
        if handler.close and isinstance(handler, logging.StreamHandler):
            handler.close()
        logger.removeHandler(handler)


def code_profiler(out_dir):
    lines = [line for line in open(
        out_dir + '/dg_model.log') if 'took:' in line]

    process = [line.split('took:')[-2] for line in lines]
    process = [line.split(':')[-1] for line in process]

    time = [line.split('took:')[-1] for line in lines]
    time = [line.split('s')[0] for line in time]
    time = [float(x) for x in time]

    profile = pd.DataFrame({'process': process, 'time': time})
    profile = profile.sort_values('time', ascending=False)
    profile.to_csv(out_dir + '/code_profiler.csv')


def current_datetime(format='%Y_%m_%d_%Hh%Mm%Ss'):

    dt = datetime.datetime.strftime(datetime.datetime.now(), format)

    return dt


class Timer:
    # adapted from
    # http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start

#==============================================================================
#       Miscellaneous Functions
#==============================================================================
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
        opts, args = getopt.getopt(argv, "hiy:", ["year="])
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


def get_epoch_time():

    epoch_time = time.time()

    return epoch_time


def get_formatted_time():

    formatted_time = time.strftime('%Y%m%d_%H%M%S')

    return formatted_time
