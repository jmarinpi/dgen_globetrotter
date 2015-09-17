# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 10:51:51 2015

@author: mgleason
"""

import json

def get_pg_params(json_file):
    
    pg_params_json = file(json_file,'r')
    pg_params = json.load(pg_params_json)
    pg_params_json.close()

    pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s port=%(port)s' % pg_params

    return pg_params, pg_conn_string