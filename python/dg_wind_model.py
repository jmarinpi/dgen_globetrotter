"""
Distributed Wind Diffusion Model
National Renewable Energy Lab

@author: bsigrin
"""

# 1. # Initialize Model
import time
import os

# 2. # Import modules and global vars
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg
import psycopg2.extras as pgx
import numpy as np
import scipy as sp
import glob
import matplotlib as mpl
import collections
import diffusion_functions as diffunc
import financial_functions as finfunc
import data_functions as datfunc
reload(datfunc)
import DG_Wind_NamedRange_xl2pg as loadXL
import subprocess
import datetime
import config as cfg
import shutil

def main():
    model_init = time.time()
    print 'Initiating model at %s' %time.ctime()
    
    # 3. check that config values are acceptable

    # check that random generator seed is in the acceptable range
    if cfg.random_generator_seed < 0 or cfg.random_generator_seed > 1:
        raise ValueError("""Error: random_generator_seed in config.py is not in the range of acceptable values.
                        Change to a value in the range >= 0 and <= 1.""")
                        
    # check that number of customer bins is in the acceptable range
    if cfg.customer_bins not in (10,50,100,500):
        raise ValueError("""Error: customer_bins in config.py is not in the range of acceptable values.
                            Change to a value in the set (10,50,100,500).""")    
    
    
    # 4. Connect to Postgres and configure connection(s)
    # (to edit login information, edit config.py)
    
    # if parallelization is off, reduce npar to 1
    if not cfg.parallelize:
        cfg.npar = 1
    
    # create a single connection to Postgres Database -- this will serve as the main cursor/connection
    con, cur = datfunc.make_con(cfg.pg_conn_string)
    # register access to hstore in postgres
    pgx.register_hstore(con)
    
    # configure pandas display options
    pd.set_option('max_columns', 9999)
    pd.set_option('max_rows',10)
    
    # find the input excel spreadsheets
    if cfg.load_scenario_inputs:    
        input_scenarios = [s for s in glob.glob("../input_scenarios/*.xls*") if not '~$' in s]
        if len(input_scenarios) == 0:
            raise ValueError("""No input scenario spreadsheet were found in the input_scenarios folder.""")
    else:
        input_scenarios = ['']

    # make output results folders
    cdate = time.strftime('%Y%m%d_%H%M%S')    
    out_dir = '%s/runs/results_%s' %(os.path.dirname(os.getcwd()),cdate)        
    os.makedirs(out_dir)

    # run the model for each input scenario spreadsheet
    scenario_names = []
    out_subfolders = []
    for i, input_scenario in enumerate(input_scenarios):
        print '\n------------------------------------------------------------------'
        print "Running Scenario %s of %s" % (i+1, len(input_scenarios))        
        
        # 5. Load Input excel spreadsheet to Postgres
        if cfg.load_scenario_inputs:
            print 'Loading input data from Input Scenario Worksheet'
            try:
                loadXL.main(input_scenario, con, verbose = False)
            except loadXL.ExcelError, e:
                print 'Loading failed with the following error: %s' % e
                print 'Model aborted'
                sys.exit(-1)
        else:
            print "Warning: Skipping Import of Input Scenario Worksheet. This should only be done while testing."
        
        
        # 6. Read in scenario option variables 
        scenario_opts = datfunc.get_scenario_options(cur) 
        exclusions = datfunc.get_exclusions(cur) # get exclusions
        load_growth_scenario = scenario_opts['load_growth_scenario'] # get financial variables
        net_metering = scenario_opts['net_metering_availability']
        inflation = scenario_opts['ann_inflation']
        
        # start year comes from config
        end_year = scenario_opts['end_year']
        model_years = range(cfg.start_year,end_year+1,2)
        
        # get the sectors to model
        sectors = datfunc.get_sectors(cur)
        
        deprec_schedule = datfunc.get_depreciation_schedule(con, type = 'standard').values
        financial_parameters = datfunc.get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned')
        max_market_share = datfunc.get_max_market_share(con, sectors.values(), scenario_opts, residential_type = 'retrofit', commercial_type = 'retrofit', industrial_type = 'retrofit')
        market_projections = datfunc.get_market_projections(con)
        
        # 7. Combine All of the Temporally Varying Data in a new Table in Postgres
        datfunc.combine_temporal_data(cur, con, cfg.start_year, end_year, datfunc.pylist_2_pglist(sectors.values()), cfg.preprocess)
        
        # 8. Set up the Main Data Frame for each sector
        outputs = pd.DataFrame()
        datfunc.clear_outputs(con,cur) # clear results from previous run 
        for sector_abbr, sector in sectors.iteritems():
            # define the rate escalation source and max market curve for the current sector
            rate_escalation_source = scenario_opts['%s_rate_escalation' % sector_abbr]
            max_market_curve = scenario_opts['%s_max_market_curve' % sector_abbr]
            # create the Main Table in Postgres (optimal turbine size and height for each year and customer bin)
            t0 = time.time()
            main_table = datfunc.generate_customer_bins(cur, con, cfg.random_generator_seed, cfg.customer_bins, sector_abbr, sector, 
                                           cfg.start_year, end_year, rate_escalation_source, load_growth_scenario, exclusions,
                                           cfg.oversize_turbine_factor, cfg.undersize_turbine_factor, cfg.preprocess, cfg.npar, cfg.pg_conn_string)
            print time.time()-t0
        
            # get dsire incentives for the generated customer bins
            t0 = time.time()
            dsire_incentives = datfunc.get_dsire_incentives(cur, con, sector_abbr, cfg.preprocess, cfg.npar, cfg.pg_conn_string)
            print time.time()-t0
            # Pull data from the Main Table to a Data Frame for each year
            
            for year in model_years:
                print 'Working on %s for %s sector' %(year, sector_abbr) 
                df = datfunc.get_main_dataframe(con, main_table, year)
                df['sector'] = sector.lower()
                df = pd.merge(df,market_projections[['year', 'customer_expec_elec_rates']], how = 'left', on = 'year')
                df = pd.merge(df,financial_parameters, how = 'left', on = 'sector')
                
                ## Diffusion from previous year ## 
                if year == cfg.start_year: 
                    # get the initial market share per bin by county
                    initial_market_shares = datfunc.get_initial_market_shares(cur, con, sector_abbr, sector)
                    # join this to the df to on county_id
                    df = pd.merge(df, initial_market_shares, how = 'left', on = 'gid')
                    df['market_value_last_year'] = df['installed_capacity_last_year'] * df['installed_costs_dollars_per_kw']        
                else:
                    df = pd.merge(df,market_last_year, how = 'left', on = 'gid')
               
                # 9. Calculate economics including incentives
                # Calculate value of incentives. Manual and DSIRE incentives can't stack. DSIRE ptc/pbi/fit are assumed to disburse over 10 years. 
                if scenario_opts['overwrite_exist_inc']:
                    value_of_incentives = datfunc.calc_manual_incentives(df,con, year)
                else:
                    inc = pd.merge(df,dsire_incentives,how = 'left', on = 'gid')
                    value_of_incentives = datfunc.calc_dsire_incentives(inc, year, default_exp_yr = 2016, assumed_duration = 10)
                df = pd.merge(df, value_of_incentives, how = 'left', on = 'gid')
                
                revenue, costs, cfs = finfunc.calc_cashflows(df,deprec_schedule,  yrs = 30)      
                                
                #Disabled at moment because of computation time
                #df['irr'] = finfunc.calc_irr(cfs)
                #df['mirr'] = finfunc.calc_mirr(cfs, finance_rate = df.discount_rate, reinvest_rate = df.discount_rate + 0.02)
                #df['npv'] = finfunc.calc_npv(cfs,df.discount_rate)
                
                payback = finfunc.calc_payback(cfs)
                ttd = finfunc.calc_ttd(cfs, df)  
        
                df['payback_period'] = np.where(df['sector'] == 'residential',payback, ttd)
                df['lcoe'] = finfunc.calc_lcoe(costs,df.aep.values, df.discount_rate)
                df['payback_key'] = (df['payback_period']*10).astype(int)
                df = pd.merge(df,max_market_share, how = 'left', on = ['sector', 'payback_key'])
                
                # 10. Calulate diffusion
                # replace above with the following:
                df['diffusion_market_share'] = diffunc.calc_diffusion(df.payback_period.values,df.max_market_share.values, df.market_share_last_year.values)
                df['market_share'] = np.maximum(df['diffusion_market_share'], df['market_share_last_year'])
                df['new_market_share'] = df['market_share']-df['market_share_last_year']
                df['new_adopters'] = df['new_market_share'] * df['customers_in_bin']
                df['new_capacity'] = df['new_adopters'] * df['nameplate_capacity_kw']
                df['new_market_value'] = df['new_adopters'] * df['nameplate_capacity_kw'] * df['installed_costs_dollars_per_kw']
                # then add these values to values from last year to get cumulative values:
                df['number_of_adopters'] = df['number_of_adopters_last_year'] + df['new_adopters']
                df['installed_capacity'] = df['installed_capacity_last_year'] + df['new_capacity']
                df['market_value'] = df['market_value_last_year'] + df['new_market_value']

                
                # 11. Save outputs from this year and update parameters for next solve       
                # Save outputs
                # original method (memory intensive)
                # outputs = outputs.append(df, ignore_index = 'True')
                # postgres method
                datfunc.write_outputs(con, cur, df, sector_abbr)     
                
                market_last_year = df[['gid','market_share', 'number_of_adopters', 'installed_capacity', 'market_value']] # Update dataframe for next solve year
                market_last_year.columns = ['gid', 'market_share_last_year', 'number_of_adopters_last_year', 'installed_capacity_last_year', 'market_value_last_year' ]
        
        
        ## 12. Outputs & Visualization
        # set output subfolder
        scen_name = scenario_opts['scenario_name']
        dup_n = 1
        if scen_name in scenario_names:
            print "Warning: Scenario name %s is a duplicate. Renaming to %s_%s" % (scen_name, scen_name, dup_n)
            scen_name = "%s_%s" % (scen_name, dup_n)
            dup_n += 1
        scenario_names.append(scen_name)
        out_path = os.path.join(out_dir,scen_name)
        out_subfolders.append(out_path)
        os.makedirs(out_path)
        
        # path to the plot_outputs R script        
        plot_outputs_path = '%s/r/graphics/plot_outputs.R' % os.path.dirname(os.getcwd())        
                
        print 'Writing outputs'
        t0 = time.time()
        # original method based on in memory df
        #outputs = outputs.fillna(0)
        #outputs.to_csv(out_path + '/outputs.csv')
        # copy csv from postgres
        f = open(out_path+'/outputs.csv','w')
        cur.copy_expert('COPY (SELECT * FROM wind_ds.outputs_all) TO STDOUT WITH CSV HEADER;', f)
        f.close()
        print time.time() - t0
        
        # copy the input scenario spreadsheet
        shutil.copy(input_scenario, out_path)
        
        #command = ("%s --vanilla ../r/graphics/plot_outputs.R %s" %(Rscript_path, runpath))
        # for linux and mac, this needs to be formatted as a list of args passed to subprocess
        command = [cfg.Rscript_path,'--vanilla',plot_outputs_path,out_path,scen_name]
        print 'Creating outputs report'            
        proc = subprocess.Popen(command,stdout=subprocess.PIPE)
        messages = proc.communicate()
        returncode = proc.returncode
        print 'Model completed at %s run took %.1f seconds' %(time.ctime(), time.time() - model_init)                 
    
    # assemble report to compare scenarios
    if len(input_scenarios) > 1:
        scenario_analysis_path = '%s/r/graphics/scenario_analysis.R' % os.path.dirname(os.getcwd())
        scenario_output_paths = datfunc.pylist_2_pglist(out_subfolders).replace("'","").replace(" ","")
        scenario_comparison_path = os.path.join(out_dir,'scenario_comparison')
        command = [cfg.Rscript_path,'--vanilla',scenario_analysis_path,scenario_output_paths,scenario_comparison_path]
        print 'Creating scenario analysis report'            
        proc = subprocess.Popen(command,stdout=subprocess.PIPE)
        messages = proc.communicate()
        returncode = proc.returncode
    
    
if __name__ == '__main__':
    main()