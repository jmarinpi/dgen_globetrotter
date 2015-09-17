# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 10:54:39 2015

@author: mgleason
"""

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