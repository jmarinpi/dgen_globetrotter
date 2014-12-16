# -*- coding: utf-8 -*-
"""
Created on Wed Nov 12 09:57:08 2014

@author: mgleason
"""
import numpy as np
import sys
import os
os.chdir('/Users/mgleason/NREL_Projects/Software/ssc-sdk-2014-1-21/languages/python')
import ssc
import multiprocessing
import time

def calc_energy_value(ac_hourly):
    # INITIALIZE THE DATA CONTAINER]
    dat = ssc.Data()   
#    
    # 4 - Run the utility rate module to calculate annual values of energy
    # set params
    rate_escalation = 0
    analysis_years = 1
    system_availability = 100
    system_degradation = 0
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
    dat.set_number('analysis_years', analysis_years )
    dat.set_array('system_availability', [ system_availability ] )
    dat.set_array('system_degradation', [ system_degradation ] )
    dat.set_array('rate_escalation', [ rate_escalation ] )
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
    # load the module
    utilityrate = ssc.Module('utilityrate')
    # run the module
    utilityrate.exec_(dat)
    # free the moduel
#    ssc.module_free(utilityrate)
    # get the data we need
    energy_value = dat.get_array('energy_value')
    
    return energy_value
    
    
    
def loop_calc_energy_value(l, ac_hourly, q):
    energy_values = []
    for i in l:
        energy_value = calc_energy_value(ac_hourly)
        energy_values.append(energy_value)
    
    q.put(energy_values)
    return 0
    


def main():
    # INITIALIZE THE DATA CONTAINER FOR PV GENERATION
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
    
    
    # THIS IS A ONE TIME CALCULATION
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
    
    # RUN IN PARALLEL
#    t0 = time.time()
#    q = multiprocessing.JoinableQueue()
#    procs = []
#    total_iterations = 5000
#    cores = 2
#    split_iterations = np.array_split(np.arange(0,total_iterations), cores)
#    for ilist in split_iterations:
#        proc = multiprocessing.Process(target = loop_calc_energy_value, args =(list(ilist), ac_hourly, q))
#        procs.append(proc)
#        proc.start()
#    
#    for p in procs:
#        p.join()
#    
#    results = [q.get() for p in procs]
#    q.close()
##    q.join_thread()
#    print time.time()-t0
    
    
    # RUN IN SERIAL
    t0 = time.time()
    for i in range(0,30000):
        cfs = calc_energy_value(ac_hourly)
    print time.time()-t0

main()


