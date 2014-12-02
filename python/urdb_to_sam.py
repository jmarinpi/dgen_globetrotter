# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 16:47:13 2014

@author: mgleason
"""

import json
import urllib
import urllib2
import os


def get_urdb(rate_key):
    ### MAKE THE REQUEST TO OPENEI FOR THE RATE DATA OF THE SPECIFIED RATE KEY
    
    # API LINKS:
    # http://en.openei.org/services/doc/rest/util_rates?version=3
    # http://en.openei.org/wiki/Help:US_Utility_Rate_Database_API_Tutorial#Requesting_Utility_Company_Data_with_OpenEI_API

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
    raw['url'] = url
    raw['guid'] = rate_key
    raw['rateurl'] = "http://en.openei.org/apps/USURDB/rate/view/" + rate_key
    
    return raw
        

def save_urdb(rate_key, outfile):
    ### MAKE THE REQUEST TO OPENEI FOR THE RATE DATA OF THE SPECIFIED RATE KEY
    
    # API LINKS:
    # http://en.openei.org/services/doc/rest/util_rates?version=3
    # http://en.openei.org/wiki/Help:US_Utility_Rate_Database_API_Tutorial#Requesting_Utility_Company_Data_with_OpenEI_API

    api = 'http://en.openei.org/services/rest/utility_rates?'
    
    
    
    params = {'version' : 3,
              'format' : 'json',
              'detail' : 'full',
              'getpage': rate_key
              }
              
    params_encode = urllib.urlencode(params)
    
    url = api+params_encode
    o = urllib2.urlopen(url)
    f = file(outfile, 'w')
    f.write(o.read())
    o.close()
    f.close()
    
    return 1


def generate_field_names():
    
    ### SET UP FIELD NAMES TO EXTRACT FROM THE RAW DATA
    
    #   DESCRIPTIVE FIELDS
    #       (key is the key used by URDB, value is the key used by SAM)
    descriptive_fields = {'name' : 'ur_schedule_name',
                          'utility' : 'ur_name',
                          'source' : 'ur_source'}
                                
    #   BASIC RATE FIELDS
    #       (key is the key used by URDB, value is the key used by SAM)
    basic_rate_fields = {   'peakkwcapacityhistory' : 'ur_demand_history',
                            'peakkwcapacitymax' : 'ur_demand_max',
                            'peakkwcapacitymin' : 'ur_demand_min',
                            'peakkwhusagehistory' : 'ur_energy_history',
                            'peakkwhusagemax' : 'ur_energy_max',
                            'peakkwhusagemin' : 'ur_energy_min',
                            'voltagemaximum' : 'ur_voltage_max',
                            'voltageminimum' : 'ur_voltage_min',
                            'voltagecategory' : 'ur_voltage_category',
                            'phasewiring' : 'ur_phase_wiring',
                            'annualmincharge' : 'ur_annual_min_charge',
                            'minmonthlycharge' : 'ur_monthly_min_charge',
                            'fixedmonthlycharge' : 'ur_monthly_fixed_charge',
                            'energyweekdayschedule' : 'ur_ec_sched_weekday',
                            'energyweekendschedule' : 'ur_ec_sched_weekend',
                            'demandweekdayschedule' : 'ur_dc_sched_weekday',
                            'demandweekendschedule' : 'ur_dc_sched_weekend'}
    
#    #   COMPLEX RATE FIELDS
#    #       (this one is a list because SAM and URDB use the same names)
#    #       NOTE: The resulting list of fields was been checked against SAM SDKTool.app on 11/24/14
#    complex_rate_fields = []
#    complex_structures = {'energy_charge': {'field_template' : "ur_ec_p%s_t%s_%s", 'field_suffixes' : ['ub', 'br', 'sr']},
#                          'demand_tou'   : {'field_template' : "ur_dc_p%s_t%s_%s", 'field_suffixes' : ['ub', 'dc']},
#                          'flat_demand'  : {'field_template' : "ur_dc_%s_t%s_%s" , 'field_suffixes' : ['ub', 'dc']}
#                         }
#    months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec" ]
#    for period in range(0, 12):
#        for tier in range(1, 7):
#            for structure in complex_structures.keys():
#                if structure == 'flat_demand':
#                    period_name = months[period]
#                else:
#                    period_name = period + 1
#                field_template = complex_structures[structure]['field_template']
#                field_suffixes = complex_structures[structure]['field_suffixes']
#                for suffix in field_suffixes:
#                    key = field_template % (period_name, tier, suffix)
#                    complex_rate_fields.append(key)
    
    # combine all together
    all_fields = {}
    all_fields.update(descriptive_fields)
    all_fields.update(basic_rate_fields)
#    all_fields.update(dict((f,f) for f in complex_rate_fields))

    return all_fields
        
def extract_sam_fields(raw_json):
          
    # get the dictionary of fields
    simple_fields = generate_field_names()
    
    # initialize the output dictionary
    rate = {}
    # add each field that applies to the data pulled from openEI
    for urdb_key, sam_key in simple_fields.iteritems():
        if urdb_key in raw_json.keys():
            rate[sam_key] = raw_json[urdb_key]  
            
    
    # Manually add some additional strange ones
    # descriptive text
    rate['ur_description'] = '. '.join([str(raw_json[k]) for k in ['description','basicinformationcomments','energycomments','demandcomments'] if k in raw_json.keys()])
    
    # net metering
    # defaults to true if not specified (we will want to change this in the diffusion modeling)
    if 'usenetmetering' in raw_json.keys():
        rate['ur_enable_net_metering'] = raw_json['usenetmetering']
    else:
        rate['ur_enable_net_metering'] = True
        
    # "fixed energy rates - URDB handles as energy charges" (not sure what this means -- taken from source code)
    rate['ur_flat_buy_rate'] = 0
    rate['ur_flat_sell_rate'] = 0
    
    return rate
    

def urdb_rate_to_sam_structure(rate_key):

    raw_json = get_urdb(rate_key)
    rate = extract_sam_fields(raw_json)
    # add the rate key to the returned dictionary
    rate['urdb_rate_key'] = rate_key

    return rate
    
def json_default(json, key, default_val):
    
    if key in json.keys():
        return json[key]
    else:
        return default_val

def extract_energy_rate_structure(raw_json):
    
    field_template = "ur_ec_p%s_t%s_"
    d = {}
    ec_enable = False
    if 'energyratestructure' in raw_json.keys():
        ers_periods = raw_json['energyratestructure']
        period_count = len(ers_periods)
        if period_count > 12:
            raise ValueError('Number of periods in energyratestructure is greater than 12')
        for period in range(0, period_count):
            ers_tiers = ers_periods[period]
            if type(ers_tiers) == list:
                tier_count = len(ers_tiers)
                if tier_count > 6:
                    raise ValueError('Number of tiers in energyratestructure is greater than 6')
                for tier in range(0, tier_count):
                    field_base = field_template % (period+1, tier+1)
                    # extract by key if available, otherwise set to default
                    energy_max = json_default(ers_tiers[tier], 'max', 1e38)
                    energy_buy = json_default(ers_tiers[tier], 'rate', 0.0)
                    energy_adj = json_default(ers_tiers[tier], 'adj', 0.0)
                    energy_sell = json_default(ers_tiers[tier], 'sell', 0.0)
                    buy_rate = energy_buy + energy_adj
                    d[field_base + 'ub'] = energy_max
                    d[field_base + 'br'] = buy_rate
                    d[field_base + 'sr'] = energy_sell
                    if not(ec_enable) and buy_rate <> 0:
                        ec_enable = True
    
    d['ec_enable'] = int(ec_enable)
    
    return d

if __name__ == '__main__':
#    # set a rate key to test
#    rate_key = '53fcf1095257a3764cdbd604'
#    rate_key = '539fc4a4ec4f024c27d8c945'
#    rate_key = '539f737eec4f024411ecfd7f'
#    rate_key = '539fba9cec4f024bc1dc2feb'

#    save_urdb(rate_key,os.path.join('/Users/mgleason/NREL_Projects/Projects/URDB_Rates/test_json', '%s.json' % rate_key))
#    output = urdb_rate_to_sam_structure(rate_key)

    rate_key = '539f70f6ec4f024411ece0b1'    
    raw_json = get_urdb(rate_key)


    # this is the code for flat demand (needs further testing)
    d = {}    
    dc_enable = False
    if 'flatdemandstructure' in raw_json.keys():
        fcharges = {}
        flat_demand_maxes = {}
        fds_periods = raw_json['flatdemandstructure']
        period_count = len(fds_periods)
        if period_count > 12:
            raise ValueError('Number of periods in flatdemandstructure is greater than 12')
        for period in range(0, period_count):
            fds_tiers = fds_periods[period]
            if type(fds_tiers) == list:
                fcharges[period] = {}
                flat_demand_maxes[period] = {}
                tier_count = len(fds_tiers)
                if tier_count > 6:
                    raise ValueError('Number of tiers in flatdemandstructure is greater than 6')               
                for tier in range(0, tier_count):
                    # extract by key if available, otherwise set to default            
                    flat_demand_max = json_default(fds_tiers[tier], 'max', 1e38)
                    flat_demand_charge = json_default(fds_tiers[tier], 'rate', 0)
                    flat_demand_adj = json_default(fds_tiers[tier], 'adj', 0)
                    fcharge = flat_demand_charge + flat_demand_adj
                    # assign to dictionary
                    fcharges[period][tier] = fcharge
                    flat_demand_maxes[period][tier] = flat_demand_max
                    if not(dc_enable) and fcharge <> 0:
                        dc_enable = True

    months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec" ]
    field_template = "ur_dc_%s_t%s_"
    if 'flatdemandmonths' in raw_json.keys():
        fdm_periods = raw_json['flatdemandmonths']
        period_count = len(fdm_periods)
        if period_count <> 12:
            raise ValueError('Number of periods in flatdemandmonths is not equal to 12') 
        flat_demand_months = dict(zip(months, fdm_periods))
    
        for month, period in flat_demand_months.iteritems():
            period_charges = fcharges[period]
            period_demand_maxes = flat_demand_maxes[period]
            tiers = len(period_charges)
            for tier in range(0, tiers):
                field_base = field_template % (month, tier+1)    
                fcharge = period_charges[tier]
                flat_demand_max = period_demand_maxes[tier]
                d[field_base + 'ub'] = flat_demand_max                
                d[field_base + 'dc'] = fcharge


    # this is the code for tou demand (needs further testing)
    field_template = "ur_dc_p%s_t%s_"
    if 'demandratestructure' in raw_json.keys():
        drs_periods = raw_json['demandratestructure']
        period_count = len(drs_periods)
        if period_count > 12:
            raise ValueError('Number of periods in demandratestructure is greater than 12')
        for period in range(0, period_count):
            drs_tiers = drs_periods[period]
            if type(drs_tiers) == list:
                tier_count = len(drs_tiers)
                if tier_count > 6:
                    raise ValueError('Number of tiers in demandratestructure is greater than 6')
                for tier in range(0, tier_count):
                    field_base = field_template % (period+1, tier+1)
                    # extract by key if available, otherwise set to default            
                    demand_max = json_default(drs_tiers[tier], 'max', 1e38)
                    demand_charge = json_default(drs_tiers[tier], 'rate', 0)
                    demand_adj = json_default(drs_tiers[tier], 'adj', 0)          
                    dcharge = demand_charge + demand_adj
                    d[field_base + 'ub'] = demand_max                
                    d[field_base + 'dc'] = dcharge
                    if not(dc_enable) and dcharge <> 0:
                        dc_enable = True

    d['dc_enable'] = int(dc_enable)            
            


