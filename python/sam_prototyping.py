# -*- coding: utf-8 -*-
"""
Created on Wed Nov 12 09:57:08 2014

@author: mgleason
"""
import numpy as np
import sys
import os
os.chdir('/Users/mgleason/NREL_Projects/Software/ssc-sdk-2014-1-21/languages/python')
#sys.path.append()
#sys.path.append('/Users/mgleason/NREL_Projects/Software/ssc-sdk-2014-1-21/osx64')
import ssc


def calc_cashflows(ac_hourly):
    # INITIALIZE THE DATA CONTAINER
    dat = ssc.Data()
    
    # NOTE: THIS IS THE SLOWEST PART -- MOVE TO OUTSIDE OF FUNCTION AND PASS IN GENERATION VALUES
    # 1 - add weather file to dat
    # set params
#    wf_path = '/Users/mgleason/NREL_Projects/Software/ssc-sdk-2014-1-21/examples/AZ_Phoenix.tm2'
#    dat.set_string('file_name', wf_path)
#    # create the module
#    wfreader = ssc.Module('wfreader')
#    # run the module
#    wfreader.exec_(dat)
    # free the module
#    ssc.module_free(wfreader)
    
    
#    # 2- calculate hourly energy production for a system
#    # set pv system params
#    dat.set_number('system_size', 4)
#    dat.set_number('derate', 0.85)
#    dat.set_number('track_mode', 0)
#    dat.set_number('tilt', 40)
#    dat.set_number('azimuth', 180)
#    dat.set_number('albedo',0)
#    # create the module
#    pv_watts = ssc.Module('pvwattsv1')
#    # run the module
#    pv_watts.exec_(dat)
#    # free the module
##    ssc.module_free(pv_watts)
#    # extract the hourly generation data
#    ac_hourly = dat.get_array('ac')
    
    # 3 - calculate annuale energy output under projected parameters
    # NOTE: I DONT THINK THIS IS NECESARRY FOR OUR ANALYSIS
    # set params
#    dat.set_number('analysis_years', 30)
#    dat.set_array('energy_availability', [100])
#    dat.set_array('energy_degradation', [0.5]*30)
#    dat.set_matrix('energy_curtailment', np.ones((12,24)))
#    dat.set_number('system_use_lifetime_output', 0)
#    dat.set_array('energy_net_hourly', ac_hourly) # this assumes no hourly load
#    # create the module
#    annualoutput = ssc.Module('annualoutput')
#    # run the module
#    annualoutput.exec_(dat)
#    # free the module
##    ssc.module_free(annualoutput)
#    # extract the data we neeed 
#    annual_e_net_delivered = dat.get_array('annual_e_net_delivered')
#    
    # 4 - Run the utility rate module to calculate annual values of energy
    # set params
    rate_escalation = 0
    ur_sell_eq_buy = 1
    ur_monthly_fixed_charge = 0
    ur_flat_buy_rate = 0.12
    ur_flat_sell_rate = 0
    ur_tou_enable = 0
    ur_tou_p1_buy_rate = 0.12
    ur_tou_p1_sell_rate = 0
    ur_tou_p2_buy_rate = 0.12
    ur_tou_p2_sell_rate = 0
    ur_tou_p3_buy_rate = 0.12
    ur_tou_p3_sell_rate = 0
    ur_tou_p4_buy_rate = 0.12
    ur_tou_p4_sell_rate = 0
    ur_tou_p5_buy_rate = 0.12
    ur_tou_p5_sell_rate = 0
    ur_tou_p6_buy_rate = 0.12
    ur_tou_p6_sell_rate = 0
    ur_tou_p7_buy_rate = 0.12
    ur_tou_p7_sell_rate = 0
    ur_tou_p8_buy_rate = 0.12
    ur_tou_p8_sell_rate = 0
    ur_tou_p9_buy_rate = 0.12
    ur_tou_p9_sell_rate = 0
    ur_tou_sched_weekday = '111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111'
    ur_tou_sched_weekend = '111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111'
    ur_dc_enable = 0
    ur_dc_fixed_m1 = 0
    ur_dc_fixed_m2 = 0
    ur_dc_fixed_m3 = 0
    ur_dc_fixed_m4 = 0
    ur_dc_fixed_m5 = 0
    ur_dc_fixed_m6 = 0
    ur_dc_fixed_m7 = 0
    ur_dc_fixed_m8 = 0
    ur_dc_fixed_m9 = 0
    ur_dc_fixed_m10 = 0
    ur_dc_fixed_m11 = 0
    ur_dc_fixed_m12 = 0
    ur_dc_p1 = 0
    ur_dc_p2 = 0
    ur_dc_p3 = 0
    ur_dc_p4 = 0
    ur_dc_p5 = 0
    ur_dc_p6 = 0
    ur_dc_p7 = 0
    ur_dc_p8 = 0
    ur_dc_p9 = 0
    ur_dc_sched_weekday = '444444443333333333334444444444443333333333334444444444443333333333334444444444443333333333334444222222221111111111112222222222221111111111112222222222221111111111112222222222221111111111112222222222221111111111112222222222221111111111112222444444443333333333334444444444443333333333334444'
    ur_dc_sched_weekend = '444444443333333333334444444444443333333333334444444444443333333333334444444444443333333333334444222222221111111111112222222222221111111111112222222222221111111111112222222222221111111111112222222222221111111111112222222222221111111111112222444444443333333333334444444444443333333333334444'
    ur_tr_enable = 0
    ur_tr_sell_mode = 1
    ur_tr_sell_rate = 0
    ur_tr_s1_energy_ub1 = 1e+099
    ur_tr_s1_energy_ub2 = 1e+099
    ur_tr_s1_energy_ub3 = 1e+099
    ur_tr_s1_energy_ub4 = 1e+099
    ur_tr_s1_energy_ub5 = 1e+099
    ur_tr_s1_energy_ub6 = 1e+099
    ur_tr_s1_rate1 = 0
    ur_tr_s1_rate2 = 0
    ur_tr_s1_rate3 = 0
    ur_tr_s1_rate4 = 0
    ur_tr_s1_rate5 = 0
    ur_tr_s1_rate6 = 0
    ur_tr_s2_energy_ub1 = 1e+099
    ur_tr_s2_energy_ub2 = 1e+099
    ur_tr_s2_energy_ub3 = 1e+099
    ur_tr_s2_energy_ub4 = 1e+099
    ur_tr_s2_energy_ub5 = 1e+099
    ur_tr_s2_energy_ub6 = 1e+099
    ur_tr_s2_rate1 = 0
    ur_tr_s2_rate2 = 0
    ur_tr_s2_rate3 = 0
    ur_tr_s2_rate4 = 0
    ur_tr_s2_rate5 = 0
    ur_tr_s2_rate6 = 0
    ur_tr_s3_energy_ub1 = 1e+099
    ur_tr_s3_energy_ub2 = 1e+099
    ur_tr_s3_energy_ub3 = 1e+099
    ur_tr_s3_energy_ub4 = 1e+099
    ur_tr_s3_energy_ub5 = 1e+099
    ur_tr_s3_energy_ub6 = 1e+099
    ur_tr_s3_rate1 = 0
    ur_tr_s3_rate2 = 0
    ur_tr_s3_rate3 = 0
    ur_tr_s3_rate4 = 0
    ur_tr_s3_rate5 = 0
    ur_tr_s3_rate6 = 0
    ur_tr_s4_energy_ub1 = 1e+099
    ur_tr_s4_energy_ub2 = 1e+099
    ur_tr_s4_energy_ub3 = 1e+099
    ur_tr_s4_energy_ub4 = 1e+099
    ur_tr_s4_energy_ub5 = 1e+099
    ur_tr_s4_energy_ub6 = 1e+099
    ur_tr_s4_rate1 = 0
    ur_tr_s4_rate2 = 0
    ur_tr_s4_rate3 = 0
    ur_tr_s4_rate4 = 0
    ur_tr_s4_rate5 = 0
    ur_tr_s4_rate6 = 0
    ur_tr_s5_energy_ub1 = 1e+099
    ur_tr_s5_energy_ub2 = 1e+099
    ur_tr_s5_energy_ub3 = 1e+099
    ur_tr_s5_energy_ub4 = 1e+099
    ur_tr_s5_energy_ub5 = 1e+099
    ur_tr_s5_energy_ub6 = 1e+099
    ur_tr_s5_rate1 = 0
    ur_tr_s5_rate2 = 0
    ur_tr_s5_rate3 = 0
    ur_tr_s5_rate4 = 0
    ur_tr_s5_rate5 = 0
    ur_tr_s5_rate6 = 0
    ur_tr_s6_energy_ub1 = 1e+099
    ur_tr_s6_energy_ub2 = 1e+099
    ur_tr_s6_energy_ub3 = 1e+099
    ur_tr_s6_energy_ub4 = 1e+099
    ur_tr_s6_energy_ub5 = 1e+099
    ur_tr_s6_energy_ub6 = 1e+099
    ur_tr_s6_rate1 = 0
    ur_tr_s6_rate2 = 0
    ur_tr_s6_rate3 = 0
    ur_tr_s6_rate4 = 0
    ur_tr_s6_rate5 = 0
    ur_tr_s6_rate6 = 0
    ur_tr_sched_m1 = 0
    ur_tr_sched_m2 = 0
    ur_tr_sched_m3 = 0
    ur_tr_sched_m4 = 0
    ur_tr_sched_m5 = 0
    ur_tr_sched_m6 = 0
    ur_tr_sched_m7 = 0
    ur_tr_sched_m8 = 0
    ur_tr_sched_m9 = 0
    ur_tr_sched_m10 = 0
    ur_tr_sched_m11 = 0
    ur_tr_sched_m12 = 0
    t1 = time.time()
    dat.set_number('analysis_years', 30 )
    dat.set_array('system_availability', [ 100 ] )
    dat.set_array('system_degradation', [ 0.5 ] )
    dat.set_array('rate_escalation', [ 0.5+2.5 ] )
    dat.set_number('ur_sell_eq_buy', ur_sell_eq_buy )
    dat.set_number('ur_monthly_fixed_charge', ur_monthly_fixed_charge )
    dat.set_number('ur_flat_buy_rate', ur_flat_buy_rate )
    dat.set_number('ur_flat_sell_rate', ur_flat_sell_rate )
    dat.set_number('ur_tou_enable', ur_tou_enable )
    dat.set_number('ur_tou_p1_buy_rate', ur_tou_p1_buy_rate )
    dat.set_number('ur_tou_p1_sell_rate', ur_tou_p1_sell_rate )
    dat.set_number('ur_tou_p2_buy_rate', ur_tou_p2_buy_rate )
    dat.set_number('ur_tou_p2_sell_rate', ur_tou_p2_sell_rate )
    dat.set_number('ur_tou_p3_buy_rate', ur_tou_p3_buy_rate )
    dat.set_number('ur_tou_p3_sell_rate', ur_tou_p3_sell_rate )
    dat.set_number('ur_tou_p4_buy_rate', ur_tou_p4_buy_rate )
    dat.set_number('ur_tou_p4_sell_rate', ur_tou_p4_sell_rate )
    dat.set_number('ur_tou_p5_buy_rate', ur_tou_p5_buy_rate )
    dat.set_number('ur_tou_p5_sell_rate', ur_tou_p5_sell_rate )
    dat.set_number('ur_tou_p6_buy_rate', ur_tou_p6_buy_rate )
    dat.set_number('ur_tou_p6_sell_rate', ur_tou_p6_sell_rate )
    dat.set_number('ur_tou_p7_buy_rate', ur_tou_p7_buy_rate )
    dat.set_number('ur_tou_p7_sell_rate', ur_tou_p7_sell_rate )
    dat.set_number('ur_tou_p8_buy_rate', ur_tou_p8_buy_rate )
    dat.set_number('ur_tou_p8_sell_rate', ur_tou_p8_sell_rate )
    dat.set_number('ur_tou_p9_buy_rate', ur_tou_p9_buy_rate )
    dat.set_number('ur_tou_p9_sell_rate', ur_tou_p9_sell_rate )
    dat.set_string('ur_tou_sched_weekday', ur_tou_sched_weekday )
    dat.set_string('ur_tou_sched_weekend', ur_tou_sched_weekend )
    dat.set_number('ur_dc_enable', ur_dc_enable )
    dat.set_number('ur_dc_fixed_m1', ur_dc_fixed_m1 )
    dat.set_number('ur_dc_fixed_m2', ur_dc_fixed_m2 )
    dat.set_number('ur_dc_fixed_m3', ur_dc_fixed_m3 )
    dat.set_number('ur_dc_fixed_m4', ur_dc_fixed_m4 )
    dat.set_number('ur_dc_fixed_m5', ur_dc_fixed_m5 )
    dat.set_number('ur_dc_fixed_m6', ur_dc_fixed_m6 )
    dat.set_number('ur_dc_fixed_m7', ur_dc_fixed_m7 )
    dat.set_number('ur_dc_fixed_m8', ur_dc_fixed_m8 )
    dat.set_number('ur_dc_fixed_m9', ur_dc_fixed_m9 )
    dat.set_number('ur_dc_fixed_m10', ur_dc_fixed_m10 )
    dat.set_number('ur_dc_fixed_m11', ur_dc_fixed_m11 )
    dat.set_number('ur_dc_fixed_m12', ur_dc_fixed_m12 )
    dat.set_number('ur_dc_p1', ur_dc_p1 )
    dat.set_number('ur_dc_p2', ur_dc_p2 )
    dat.set_number('ur_dc_p3', ur_dc_p3 )
    dat.set_number('ur_dc_p4', ur_dc_p4 )
    dat.set_number('ur_dc_p5', ur_dc_p5 )
    dat.set_number('ur_dc_p6', ur_dc_p6 )
    dat.set_number('ur_dc_p7', ur_dc_p7 )
    dat.set_number('ur_dc_p8', ur_dc_p8 )
    dat.set_number('ur_dc_p9', ur_dc_p9 )
    dat.set_string('ur_dc_sched_weekday', ur_dc_sched_weekday )
    dat.set_string('ur_dc_sched_weekend', ur_dc_sched_weekend )
    dat.set_number('ur_tr_enable', ur_tr_enable )
    dat.set_number('ur_tr_sell_mode', ur_tr_sell_mode )
    dat.set_number('ur_tr_sell_rate', ur_tr_sell_rate )
    dat.set_number('ur_tr_s1_energy_ub1', ur_tr_s1_energy_ub1 )
    dat.set_number('ur_tr_s1_energy_ub2', ur_tr_s1_energy_ub2 )
    dat.set_number('ur_tr_s1_energy_ub3', ur_tr_s1_energy_ub3 )
    dat.set_number('ur_tr_s1_energy_ub4', ur_tr_s1_energy_ub4 )
    dat.set_number('ur_tr_s1_energy_ub5', ur_tr_s1_energy_ub5 )
    dat.set_number('ur_tr_s1_energy_ub6', ur_tr_s1_energy_ub6 )
    dat.set_number('ur_tr_s1_rate1', ur_tr_s1_rate1 )
    dat.set_number('ur_tr_s1_rate2', ur_tr_s1_rate2 )
    dat.set_number('ur_tr_s1_rate3', ur_tr_s1_rate3 )
    dat.set_number('ur_tr_s1_rate4', ur_tr_s1_rate4 )
    dat.set_number('ur_tr_s1_rate5', ur_tr_s1_rate5 )
    dat.set_number('ur_tr_s1_rate6', ur_tr_s1_rate6 )
    dat.set_number('ur_tr_s2_energy_ub1', ur_tr_s2_energy_ub1 )
    dat.set_number('ur_tr_s2_energy_ub2', ur_tr_s2_energy_ub2 )
    dat.set_number('ur_tr_s2_energy_ub3', ur_tr_s2_energy_ub3 )
    dat.set_number('ur_tr_s2_energy_ub4', ur_tr_s2_energy_ub4 )
    dat.set_number('ur_tr_s2_energy_ub5', ur_tr_s2_energy_ub5 )
    dat.set_number('ur_tr_s2_energy_ub6', ur_tr_s2_energy_ub6 )
    dat.set_number('ur_tr_s2_rate1', ur_tr_s2_rate1 )
    dat.set_number('ur_tr_s2_rate2', ur_tr_s2_rate2 )
    dat.set_number('ur_tr_s2_rate3', ur_tr_s2_rate3 )
    dat.set_number('ur_tr_s2_rate4', ur_tr_s2_rate4 )
    dat.set_number('ur_tr_s2_rate5', ur_tr_s2_rate5 )
    dat.set_number('ur_tr_s2_rate6', ur_tr_s2_rate6 )
    dat.set_number('ur_tr_s3_energy_ub1', ur_tr_s3_energy_ub1 )
    dat.set_number('ur_tr_s3_energy_ub2', ur_tr_s3_energy_ub2 )
    dat.set_number('ur_tr_s3_energy_ub3', ur_tr_s3_energy_ub3 )
    dat.set_number('ur_tr_s3_energy_ub4', ur_tr_s3_energy_ub4 )
    dat.set_number('ur_tr_s3_energy_ub5', ur_tr_s3_energy_ub5 )
    dat.set_number('ur_tr_s3_energy_ub6', ur_tr_s3_energy_ub6 )
    dat.set_number('ur_tr_s3_rate1', ur_tr_s3_rate1 )
    dat.set_number('ur_tr_s3_rate2', ur_tr_s3_rate2 )
    dat.set_number('ur_tr_s3_rate3', ur_tr_s3_rate3 )
    dat.set_number('ur_tr_s3_rate4', ur_tr_s3_rate4 )
    dat.set_number('ur_tr_s3_rate5', ur_tr_s3_rate5 )
    dat.set_number('ur_tr_s3_rate6', ur_tr_s3_rate6 )
    dat.set_number('ur_tr_s4_energy_ub1', ur_tr_s4_energy_ub1 )
    dat.set_number('ur_tr_s4_energy_ub2', ur_tr_s4_energy_ub2 )
    dat.set_number('ur_tr_s4_energy_ub3', ur_tr_s4_energy_ub3 )
    dat.set_number('ur_tr_s4_energy_ub4', ur_tr_s4_energy_ub4 )
    dat.set_number('ur_tr_s4_energy_ub5', ur_tr_s4_energy_ub5 )
    dat.set_number('ur_tr_s4_energy_ub6', ur_tr_s4_energy_ub6 )
    dat.set_number('ur_tr_s4_rate1', ur_tr_s4_rate1 )
    dat.set_number('ur_tr_s4_rate2', ur_tr_s4_rate2 )
    dat.set_number('ur_tr_s4_rate3', ur_tr_s4_rate3 )
    dat.set_number('ur_tr_s4_rate4', ur_tr_s4_rate4 )
    dat.set_number('ur_tr_s4_rate5', ur_tr_s4_rate5 )
    dat.set_number('ur_tr_s4_rate6', ur_tr_s4_rate6 )
    dat.set_number('ur_tr_s5_energy_ub1', ur_tr_s5_energy_ub1 )
    dat.set_number('ur_tr_s5_energy_ub2', ur_tr_s5_energy_ub2 )
    dat.set_number('ur_tr_s5_energy_ub3', ur_tr_s5_energy_ub3 )
    dat.set_number('ur_tr_s5_energy_ub4', ur_tr_s5_energy_ub4 )
    dat.set_number('ur_tr_s5_energy_ub5', ur_tr_s5_energy_ub5 )
    dat.set_number('ur_tr_s5_energy_ub6', ur_tr_s5_energy_ub6 )
    dat.set_number('ur_tr_s5_rate1', ur_tr_s5_rate1 )
    dat.set_number('ur_tr_s5_rate2', ur_tr_s5_rate2 )
    dat.set_number('ur_tr_s5_rate3', ur_tr_s5_rate3 )
    dat.set_number('ur_tr_s5_rate4', ur_tr_s5_rate4 )
    dat.set_number('ur_tr_s5_rate5', ur_tr_s5_rate5 )
    dat.set_number('ur_tr_s5_rate6', ur_tr_s5_rate6 )
    dat.set_number('ur_tr_s6_energy_ub1', ur_tr_s6_energy_ub1 )
    dat.set_number('ur_tr_s6_energy_ub2', ur_tr_s6_energy_ub2 )
    dat.set_number('ur_tr_s6_energy_ub3', ur_tr_s6_energy_ub3 )
    dat.set_number('ur_tr_s6_energy_ub4', ur_tr_s6_energy_ub4 )
    dat.set_number('ur_tr_s6_energy_ub5', ur_tr_s6_energy_ub5 )
    dat.set_number('ur_tr_s6_energy_ub6', ur_tr_s6_energy_ub6 )
    dat.set_number('ur_tr_s6_rate1', ur_tr_s6_rate1 )
    dat.set_number('ur_tr_s6_rate2', ur_tr_s6_rate2 )
    dat.set_number('ur_tr_s6_rate3', ur_tr_s6_rate3 )
    dat.set_number('ur_tr_s6_rate4', ur_tr_s6_rate4 )
    dat.set_number('ur_tr_s6_rate5', ur_tr_s6_rate5 )
    dat.set_number('ur_tr_s6_rate6', ur_tr_s6_rate6 )
    dat.set_number('ur_tr_sched_m1', ur_tr_sched_m1 )
    dat.set_number('ur_tr_sched_m2', ur_tr_sched_m2 )
    dat.set_number('ur_tr_sched_m3', ur_tr_sched_m3 )
    dat.set_number('ur_tr_sched_m4', ur_tr_sched_m4 )
    dat.set_number('ur_tr_sched_m5', ur_tr_sched_m5 )
    dat.set_number('ur_tr_sched_m6', ur_tr_sched_m6 )
    dat.set_number('ur_tr_sched_m7', ur_tr_sched_m7 )
    dat.set_number('ur_tr_sched_m8', ur_tr_sched_m8 )
    dat.set_number('ur_tr_sched_m9', ur_tr_sched_m9 )
    dat.set_number('ur_tr_sched_m10', ur_tr_sched_m10 )
    dat.set_number('ur_tr_sched_m11', ur_tr_sched_m11 )
    dat.set_number('ur_tr_sched_m12', ur_tr_sched_m12 )
    dat.set_array('e_with_system', ac_hourly )
    dat.set_array('e_without_system', [0.1]*8760 )
    dat.set_array('load_escalation', [ 0.2 ] )
    print time.time() - t1
    # load the module
    t1 = time.time()
    utilityrate = ssc.Module('utilityrate')
    print time.time() - t1
    # run the module
    t1 = time.time()
    utilityrate.exec_(dat)
    print time.time() - t1
    # free the moduel
#    ssc.module_free(utilityrate)
    # get the data we need
    t1 = time.time()
    energy_value = dat.get_array('energy_value')
    print time.time() - t1
    
    return energy_value
    
    
    
#    # 5 - calculate cash flows based on taxes, incentives, system costs, financing, and other stuff
#    # set params
#    federal_tax_rate = 28
#    state_tax_rate = 7
#    property_tax_rate = 0
#    prop_tax_cost_assessed_percent = 100
#    prop_tax_assessed_decline = 0
#    sales_tax_rate = 5
#    real_discount_rate = 8
#    inflation_rate = 2.5
#    insurance_rate = 0
#    system_capacity = 17.22
#    system_heat_rate = 0
#    om_fixed = 0
#    om_fixed_escal = 0
#    om_production = 0
#    om_production_escal = 0
#    om_capacity = 20
#    om_capacity_escal = 0
#    om_fuel_cost = 0
#    om_fuel_cost_escal = 0
#    annual_fuel_usage = 0
#    # incentive
#    itc_fed_amount = 0
#    itc_fed_amount_deprbas_fed = 0
#    itc_fed_amount_deprbas_sta = 0
#    itc_sta_amount = 0
#    itc_sta_amount_deprbas_fed = 0
#    itc_sta_amount_deprbas_sta = 0
#    itc_fed_percent = 30
#    itc_fed_percent_maxvalue = 1e+099
#    itc_fed_percent_deprbas_fed = 0
#    itc_fed_percent_deprbas_sta = 0
#    itc_sta_percent = 0
#    itc_sta_percent_maxvalue = 1e+099
#    itc_sta_percent_deprbas_fed = 0
#    itc_sta_percent_deprbas_sta = 0
#    ptc_fed_amount = 0
#    ptc_fed_term = 10
#    ptc_fed_escal = 2
#    ptc_sta_amount = 0
#    ptc_sta_term = 10
#    ptc_sta_escal = 2
#    ibi_fed_amount = 0
#    ibi_fed_amount_tax_fed = 1
#    ibi_fed_amount_tax_sta = 1
#    ibi_fed_amount_deprbas_fed = 0
#    ibi_fed_amount_deprbas_sta = 0
#    ibi_sta_amount = 0
#    ibi_sta_amount_tax_fed = 1
#    ibi_sta_amount_tax_sta = 1
#    ibi_sta_amount_deprbas_fed = 0
#    ibi_sta_amount_deprbas_sta = 0
#    ibi_uti_amount = 0
#    ibi_uti_amount_tax_fed = 1
#    ibi_uti_amount_tax_sta = 1
#    ibi_uti_amount_deprbas_fed = 0
#    ibi_uti_amount_deprbas_sta = 0
#    ibi_oth_amount = 0
#    ibi_oth_amount_tax_fed = 1
#    ibi_oth_amount_tax_sta = 1
#    ibi_oth_amount_deprbas_fed = 0
#    ibi_oth_amount_deprbas_sta = 0
#    ibi_fed_percent = 0
#    ibi_fed_percent_maxvalue = 1e+099
#    ibi_fed_percent_tax_fed = 1
#    ibi_fed_percent_tax_sta = 1
#    ibi_fed_percent_deprbas_fed = 0
#    ibi_fed_percent_deprbas_sta = 0
#    ibi_sta_percent = 0
#    ibi_sta_percent_maxvalue = 1e+099
#    ibi_sta_percent_tax_fed = 1
#    ibi_sta_percent_tax_sta = 1
#    ibi_sta_percent_deprbas_fed = 0
#    ibi_sta_percent_deprbas_sta = 0
#    ibi_uti_percent = 0
#    ibi_uti_percent_maxvalue = 1e+099
#    ibi_uti_percent_tax_fed = 1
#    ibi_uti_percent_tax_sta = 1
#    ibi_uti_percent_deprbas_fed = 0
#    ibi_uti_percent_deprbas_sta = 0
#    ibi_oth_percent = 0
#    ibi_oth_percent_maxvalue = 1e+099
#    ibi_oth_percent_tax_fed = 1
#    ibi_oth_percent_tax_sta = 1
#    ibi_oth_percent_deprbas_fed = 0
#    ibi_oth_percent_deprbas_sta = 0
#    cbi_fed_amount = 0
#    cbi_fed_maxvalue = 1e+099
#    cbi_fed_tax_fed = 1
#    cbi_fed_tax_sta = 1
#    cbi_fed_deprbas_fed = 0
#    cbi_fed_deprbas_sta = 0
#    cbi_sta_amount = 0
#    cbi_sta_maxvalue = 1e+099
#    cbi_sta_tax_fed = 1
#    cbi_sta_tax_sta = 1
#    cbi_sta_deprbas_fed = 0
#    cbi_sta_deprbas_sta = 0
#    cbi_uti_amount = 0
#    cbi_uti_maxvalue = 1e+099
#    cbi_uti_tax_fed = 1
#    cbi_uti_tax_sta = 1
#    cbi_uti_deprbas_fed = 0
#    cbi_uti_deprbas_sta = 0
#    cbi_oth_amount = 0
#    cbi_oth_maxvalue = 1e+099
#    cbi_oth_tax_fed = 1
#    cbi_oth_tax_sta = 1
#    cbi_oth_deprbas_fed = 0
#    cbi_oth_deprbas_sta = 0
#    pbi_fed_amount = 0
#    pbi_fed_term = 0
#    pbi_fed_escal = 0
#    pbi_fed_tax_fed = 1
#    pbi_fed_tax_sta = 1
#    pbi_sta_amount = 0
#    pbi_sta_term = 0
#    pbi_sta_escal = 0
#    pbi_sta_tax_fed = 1
#    pbi_sta_tax_sta = 1
#    pbi_uti_amount = 0
#    pbi_uti_term = 0
#    pbi_uti_escal = 0
#    pbi_uti_tax_fed = 1
#    pbi_uti_tax_sta = 1
#    pbi_oth_amount = 0
#    pbi_oth_term = 0
#    pbi_oth_escal = 0
#    pbi_oth_tax_fed = 1
#    pbi_oth_tax_sta = 1
#    total_installed_cost = 99621.1
#    salvage_percentage = 0
#    loan_debt = 100
#    loan_term = 30
#    loan_rate = 7.5
#    is_mortgage = 0 # true/fals
#    is_commercial = 0
#    # enable macrs depreciation true/false (commercial only
#    depr_fed_macrs = 1
#    depr_sta_macrs = 1
#    
#    analysis_years = 30
#    
#    dat.set_number('analysis_years', analysis_years )
#    dat.set_number('federal_tax_rate', federal_tax_rate )
#    dat.set_number('state_tax_rate', state_tax_rate )
#    dat.set_number('property_tax_rate', property_tax_rate )
#    dat.set_number('prop_tax_cost_assessed_percent', prop_tax_cost_assessed_percent )
#    dat.set_number('prop_tax_assessed_decline', prop_tax_assessed_decline )
#    dat.set_number('sales_tax_rate', sales_tax_rate )
#    dat.set_number('real_discount_rate', real_discount_rate )
#    dat.set_number('inflation_rate', inflation_rate )
#    dat.set_number('insurance_rate', insurance_rate )
#    dat.set_number('system_capacity', system_capacity )
#    dat.set_number('system_heat_rate', system_heat_rate )
#    dat.set_array('om_fixed', [ om_fixed ] )
#    dat.set_number('om_fixed_escal', om_fixed_escal )
#    dat.set_array('om_production', [ om_production ] )
#    dat.set_number('om_production_escal', om_production_escal )
#    dat.set_array('om_capacity', [ om_capacity ] )
#    dat.set_number('om_capacity_escal', om_capacity_escal )
#    dat.set_array('om_fuel_cost', [ om_fuel_cost ] )
#    dat.set_number('om_fuel_cost_escal', om_fuel_cost_escal )
#    dat.set_number('annual_fuel_usage', annual_fuel_usage )
#    dat.set_number('itc_fed_amount', itc_fed_amount )
#    dat.set_number('itc_fed_amount_deprbas_fed', itc_fed_amount_deprbas_fed )
#    dat.set_number('itc_fed_amount_deprbas_sta', itc_fed_amount_deprbas_sta )
#    dat.set_number('itc_sta_amount', itc_sta_amount )
#    dat.set_number('itc_sta_amount_deprbas_fed', itc_sta_amount_deprbas_fed )
#    dat.set_number('itc_sta_amount_deprbas_sta', itc_sta_amount_deprbas_sta )
#    dat.set_number('itc_fed_percent', itc_fed_percent )
#    dat.set_number('itc_fed_percent_maxvalue', itc_fed_percent_maxvalue )
#    dat.set_number('itc_fed_percent_deprbas_fed', itc_fed_percent_deprbas_fed )
#    dat.set_number('itc_fed_percent_deprbas_sta', itc_fed_percent_deprbas_sta )
#    dat.set_number('itc_sta_percent', itc_sta_percent )
#    dat.set_number('itc_sta_percent_maxvalue', itc_sta_percent_maxvalue )
#    dat.set_number('itc_sta_percent_deprbas_fed', itc_sta_percent_deprbas_fed )
#    dat.set_number('itc_sta_percent_deprbas_sta', itc_sta_percent_deprbas_sta )
#    dat.set_array('ptc_fed_amount', [ ptc_fed_amount ] )
#    dat.set_number('ptc_fed_term', ptc_fed_term )
#    dat.set_number('ptc_fed_escal', ptc_fed_escal )
#    dat.set_array('ptc_sta_amount', [ ptc_sta_amount ] )
#    dat.set_number('ptc_sta_term', ptc_sta_term )
#    dat.set_number('ptc_sta_escal', ptc_sta_escal )
#    dat.set_number('ibi_fed_amount', ibi_fed_amount )
#    dat.set_number('ibi_fed_amount_tax_fed', ibi_fed_amount_tax_fed )
#    dat.set_number('ibi_fed_amount_tax_sta', ibi_fed_amount_tax_sta )
#    dat.set_number('ibi_fed_amount_deprbas_fed', ibi_fed_amount_deprbas_fed )
#    dat.set_number('ibi_fed_amount_deprbas_sta', ibi_fed_amount_deprbas_sta )
#    dat.set_number('ibi_sta_amount', ibi_sta_amount )
#    dat.set_number('ibi_sta_amount_tax_fed', ibi_sta_amount_tax_fed )
#    dat.set_number('ibi_sta_amount_tax_sta', ibi_sta_amount_tax_sta )
#    dat.set_number('ibi_sta_amount_deprbas_fed', ibi_sta_amount_deprbas_fed )
#    dat.set_number('ibi_sta_amount_deprbas_sta', ibi_sta_amount_deprbas_sta )
#    dat.set_number('ibi_uti_amount', ibi_uti_amount )
#    dat.set_number('ibi_uti_amount_tax_fed', ibi_uti_amount_tax_fed )
#    dat.set_number('ibi_uti_amount_tax_sta', ibi_uti_amount_tax_sta )
#    dat.set_number('ibi_uti_amount_deprbas_fed', ibi_uti_amount_deprbas_fed )
#    dat.set_number('ibi_uti_amount_deprbas_sta', ibi_uti_amount_deprbas_sta )
#    dat.set_number('ibi_oth_amount', ibi_oth_amount )
#    dat.set_number('ibi_oth_amount_tax_fed', ibi_oth_amount_tax_fed )
#    dat.set_number('ibi_oth_amount_tax_sta', ibi_oth_amount_tax_sta )
#    dat.set_number('ibi_oth_amount_deprbas_fed', ibi_oth_amount_deprbas_fed )
#    dat.set_number('ibi_oth_amount_deprbas_sta', ibi_oth_amount_deprbas_sta )
#    dat.set_number('ibi_fed_percent', ibi_fed_percent )
#    dat.set_number('ibi_fed_percent_maxvalue', ibi_fed_percent_maxvalue )
#    dat.set_number('ibi_fed_percent_tax_fed', ibi_fed_percent_tax_fed )
#    dat.set_number('ibi_fed_percent_tax_sta', ibi_fed_percent_tax_sta )
#    dat.set_number('ibi_fed_percent_deprbas_fed', ibi_fed_percent_deprbas_fed )
#    dat.set_number('ibi_fed_percent_deprbas_sta', ibi_fed_percent_deprbas_sta )
#    dat.set_number('ibi_sta_percent', ibi_sta_percent )
#    dat.set_number('ibi_sta_percent_maxvalue', ibi_sta_percent_maxvalue )
#    dat.set_number('ibi_sta_percent_tax_fed', ibi_sta_percent_tax_fed )
#    dat.set_number('ibi_sta_percent_tax_sta', ibi_sta_percent_tax_sta )
#    dat.set_number('ibi_sta_percent_deprbas_fed', ibi_sta_percent_deprbas_fed )
#    dat.set_number('ibi_sta_percent_deprbas_sta', ibi_sta_percent_deprbas_sta )
#    dat.set_number('ibi_uti_percent', ibi_uti_percent )
#    dat.set_number('ibi_uti_percent_maxvalue', ibi_uti_percent_maxvalue )
#    dat.set_number('ibi_uti_percent_tax_fed', ibi_uti_percent_tax_fed )
#    dat.set_number('ibi_uti_percent_tax_sta', ibi_uti_percent_tax_sta )
#    dat.set_number('ibi_uti_percent_deprbas_fed', ibi_uti_percent_deprbas_fed )
#    dat.set_number('ibi_uti_percent_deprbas_sta', ibi_uti_percent_deprbas_sta )
#    dat.set_number('ibi_oth_percent', ibi_oth_percent )
#    dat.set_number('ibi_oth_percent_maxvalue', ibi_oth_percent_maxvalue )
#    dat.set_number('ibi_oth_percent_tax_fed', ibi_oth_percent_tax_fed )
#    dat.set_number('ibi_oth_percent_tax_sta', ibi_oth_percent_tax_sta )
#    dat.set_number('ibi_oth_percent_deprbas_fed', ibi_oth_percent_deprbas_fed )
#    dat.set_number('ibi_oth_percent_deprbas_sta', ibi_oth_percent_deprbas_sta )
#    dat.set_number('cbi_fed_amount', cbi_fed_amount )
#    dat.set_number('cbi_fed_maxvalue', cbi_fed_maxvalue )
#    dat.set_number('cbi_fed_tax_fed', cbi_fed_tax_fed )
#    dat.set_number('cbi_fed_tax_sta', cbi_fed_tax_sta )
#    dat.set_number('cbi_fed_deprbas_fed', cbi_fed_deprbas_fed )
#    dat.set_number('cbi_fed_deprbas_sta', cbi_fed_deprbas_sta )
#    dat.set_number('cbi_sta_amount', cbi_sta_amount )
#    dat.set_number('cbi_sta_maxvalue', cbi_sta_maxvalue )
#    dat.set_number('cbi_sta_tax_fed', cbi_sta_tax_fed )
#    dat.set_number('cbi_sta_tax_sta', cbi_sta_tax_sta )
#    dat.set_number('cbi_sta_deprbas_fed', cbi_sta_deprbas_fed )
#    dat.set_number('cbi_sta_deprbas_sta', cbi_sta_deprbas_sta )
#    dat.set_number('cbi_uti_amount', cbi_uti_amount )
#    dat.set_number('cbi_uti_maxvalue', cbi_uti_maxvalue )
#    dat.set_number('cbi_uti_tax_fed', cbi_uti_tax_fed )
#    dat.set_number('cbi_uti_tax_sta', cbi_uti_tax_sta )
#    dat.set_number('cbi_uti_deprbas_fed', cbi_uti_deprbas_fed )
#    dat.set_number('cbi_uti_deprbas_sta', cbi_uti_deprbas_sta )
#    dat.set_number('cbi_oth_amount', cbi_oth_amount )
#    dat.set_number('cbi_oth_maxvalue', cbi_oth_maxvalue )
#    dat.set_number('cbi_oth_tax_fed', cbi_oth_tax_fed )
#    dat.set_number('cbi_oth_tax_sta', cbi_oth_tax_sta )
#    dat.set_number('cbi_oth_deprbas_fed', cbi_oth_deprbas_fed )
#    dat.set_number('cbi_oth_deprbas_sta', cbi_oth_deprbas_sta )
#    dat.set_array('pbi_fed_amount', [ pbi_fed_amount ] )
#    dat.set_number('pbi_fed_term', pbi_fed_term )
#    dat.set_number('pbi_fed_escal', pbi_fed_escal )
#    dat.set_number('pbi_fed_tax_fed', pbi_fed_tax_fed )
#    dat.set_number('pbi_fed_tax_sta', pbi_fed_tax_sta )
#    dat.set_array('pbi_sta_amount', [ pbi_sta_amount ] )
#    dat.set_number('pbi_sta_term', pbi_sta_term )
#    dat.set_number('pbi_sta_escal', pbi_sta_escal )
#    dat.set_number('pbi_sta_tax_fed', pbi_sta_tax_fed )
#    dat.set_number('pbi_sta_tax_sta', pbi_sta_tax_sta )
#    dat.set_array('pbi_uti_amount', [ pbi_uti_amount ] )
#    dat.set_number('pbi_uti_term', pbi_uti_term )
#    dat.set_number('pbi_uti_escal', pbi_uti_escal )
#    dat.set_number('pbi_uti_tax_fed', pbi_uti_tax_fed )
#    dat.set_number('pbi_uti_tax_sta', pbi_uti_tax_sta )
#    dat.set_array('pbi_oth_amount', [ pbi_oth_amount ] )
#    dat.set_number('pbi_oth_term', pbi_oth_term )
#    dat.set_number('pbi_oth_escal', pbi_oth_escal )
#    dat.set_number('pbi_oth_tax_fed', pbi_oth_tax_fed )
#    dat.set_number('pbi_oth_tax_sta', pbi_oth_tax_sta )
#    dat.set_number('total_installed_cost', total_installed_cost )
#    dat.set_number('salvage_percentage', salvage_percentage )
#    
#    dat.set_number('loan_debt', loan_debt )
#    dat.set_number('loan_rate', loan_rate )
#    dat.set_number('loan_term', loan_term )
#    
#    
#    dat.set_number('market', 0 ) # residential
#    dat.set_number('mortgage', is_mortgage )
#    
#    # create the module
#    cashloan = ssc.Module('cashloan')
#    # run the module
#    cashloan.exec_(dat)
#    # free the module
##    ssc.module_free(cashloan)
#    # extract the data we need
#    cfs = dat.get_array('cf_after_tax_cash_flow')
#    # free the data
##    ssc.data_free(dat)
#    
#    return cfs








import time
# INITIALIZE THE DATA CONTAINER
pvdat = ssc.Data()


# 1 - add weather file to dat
# set params
wf_path = '/Users/mgleason/NREL_Projects/Software/ssc-sdk-2014-1-21/examples/AZ_Phoenix.tm2'
pvdat.set_string('file_name', wf_path)
# create the module
wfreader = ssc.Module('wfreader')
# run the module
wfreader.exec_(pvdat)
# free the module
#    ssc.module_free(wfreader)


# 2- calculate hourly energy production for a system
# set pv system params
pvdat.set_number('system_size', 4)
pvdat.set_number('derate', 0.85)
pvdat.set_number('track_mode', 0)
pvdat.set_number('tilt', 40)
pvdat.set_number('azimuth', 180)
pvdat.set_number('albedo',0)
# create the module
pv_watts = ssc.Module('pvwattsv1')
# run the module
pv_watts.exec_(pvdat)
# free the module
#    ssc.module_free(pv_watts)
# extract the hourly generation data
ac_hourly = pvdat.get_array('ac')




t0 = time.time()
for i in range(0,100):
    cfs = calc_cashflows(ac_hourly)
print time.time()-t0




