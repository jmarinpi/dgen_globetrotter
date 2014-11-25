# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 16:47:13 2014

@author: mgleason
"""

import json
import urllib
import urllib2


def get_urdb(rate_key):
    ### MAKE THE REQUEST TO OPENEI FOR THE RATE DATA OF THE SPECIFIED RATE KEY
    api = 'http://en.openei.org/services/rest/utility_rates?'
    
    
    
    params = {'version' : 3,
              'format' : 'json',
              'detail' : 'full',
              'getpage': rate_key
              }
              
    params_encode = urllib.urlencode(params)
    
    url = api+params_encode
    o = urllib2.urlopen(url)
    response = json.loads(o.read())
    o.close()
    
    raw = response['items'][0]
    
    return raw


def generate_field_names():
    
    ### SET UP FIELD NAMES TO EXTRACT FROM THE RAW DATA
    
    #   DESCRIPTIVE FIELDS
    #       (key is the key used by URDB, value is the key used by SAM)
    descriptive_fields = {'name' : 'ur_name',
                          'schedule_name' : 'ur_schedule_name',
                          'source' : 'ur_source'}
    
    #   BASIC RATE FIELDS
    #       (key is the key used by URDB, value is the key used by SAM)
    basic_rate_fields = {   'peakkwcapacitymin' : 'ur_demand_min',
                            'peakkwcapacitymax' : 'ur_demand_max',
                            'peakkwcapacityhistory' : 'ur_demand_history',
                            'peakkwhusagemin' : 'ur_energy_min',
                            'peakkwhusagemax' : 'ur_energy_max',
                            'peakkwhusagehistory' : 'ur_energy_history',
                            'voltageminimum' : 'ur_voltage_min',
                            'voltagemaximum' : 'ur_voltage_max',
                            'voltagecategory' : 'ur_voltage_category',
                            'phasewiring' : 'ur_phase_wiring',
                            'monthly_fixed_charge' : 'ur_monthly_fixed_charge',
                            'monthly_min_charge' : 'ur_monthly_min_charge',
                            'annual_min_charge' : 'ur_annual_min_charge',
                            'enable_net_metering' : 'ur_enable_net_metering',
                            'ec_enable' : 'ur_ec_enable',
                            'dc_enable' : 'ur_dc_enable',
                            'ec_sched_weekday' : 'ur_ec_sched_weekday',
                            'ec_sched_weekend' : 'ur_ec_sched_weekend',
                            'dc_sched_weekday' : 'ur_dc_sched_weekday',
                            'dc_sched_weekend' : 'ur_dc_sched_weekend'}
    
    #   COMPLEX RATE FIELDS
    #       (this one is a list because SAM and URDB use the same names)
    #       NOTE: The resulting list of fields was been checked against SAM SDKTool.app on 11/24/14
    complex_rate_fields = []
    complex_structures = {'energy_charge': {'field_template' : "ur_ec_p%s_t%s_%s", 'field_suffixes' : ['ub', 'br', 'sr']},
                          'demand_tou'   : {'field_template' : "ur_dc_p%s_t%s_%s", 'field_suffixes' : ['ub', 'dc']},
                          'flat_demand'  : {'field_template' : "ur_dc_%s_t%s_%s" , 'field_suffixes' : ['ub', 'dc']}
                         }
    months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec" ]
    for period in range(0, 12):
        for tier in range(1, 7):
            for structure in complex_structures.keys():
                if structure == 'flat_demand':
                    period_name = months[period]
                else:
                    period_name = period + 1
                field_template = complex_structures[structure]['field_template']
                field_suffixes = complex_structures[structure]['field_suffixes']
                for suffix in field_suffixes:
                    key = field_template % (period_name, tier, suffix)
                    complex_rate_fields.append(key)
    
    # combine all together
    all_fields = {}
    all_fields.update(descriptive_fields)
    all_fields.update(basic_rate_fields)
    all_fields.update(dict((f,f) for f in complex_rate_fields))

    return all_fields
        
def extract_sam_fields(raw_json):
          
    # get the dictionary of fields
    all_fields = generate_field_names()
    
    # initialize the output dictionary
    rate = {}
    # add each field that applies to the data pulled from openEI
    for urdb_key, sam_key in all_fields.iteritems():
        if urdb_key in raw_json.keys():
            rate[sam_key] = raw_json[urdb_key]  
    
    # Manually add some additional strange ones
    rate['ur_description'] = '. '.join([str(raw_json[k]) for k in ['description','basicinformationcomments','energycomments','demandcomments'] if k in raw_json.keys()])
    # fixed energy rates - URDB handles as energy charges
    rate['ur_flat_buy_rate'] = 0
    rate['ur_flat_sell_rate'] = 0
    
    return rate
    

def urdb_rate_to_sam_structure(rate_key):

    raw_json = get_urdb(rate_key)
    rate = extract_sam_fields(raw_json)
    # add the rate key to the returned dictionary
    rate['urdb_rate_key'] = rate_key

    return rate
    

if __name__ == '__main__':
    # set a rate key to test
    rate_key = '53fcf1095257a3764cdbd604'
    output = urdb_rate_to_sam_structure(rate_key)