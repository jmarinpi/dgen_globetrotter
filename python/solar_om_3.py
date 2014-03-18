# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 10:08:44 2013

@author: mgleason
"""

import sys

#sys.path.append('/srv/data/home/shared/pg-tools')
#import pgTools
import threading, sys, numpy as np, os
from multiprocessing import Process, Queue, JoinableQueue
from psycopg2.pool import ThreadedConnectionPool
import psycopg2 as pg

# global variables used in multiple functions
pgConnParams1 = "dbname=%s user=%s password=%s host=%s port=%s" % ('alopez','mgleason','mgleason','localhost','5432')
pgConnParams2 = "dbname=%s user=%s password=%s host=%s port=%s" % ('mgleason','mgleason','mgleason','localhost','5432')
seasons = ['winter','spring','summer','fall']
# for repeated re-use in the wfreader function, create monthly data structure (offset by one month so that winter can be sliced together)
months = ['dec','jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov']
month_days = [31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30]           
# sum up the number of days in each season by splitt month_days into 4 equal size pieces of 3 months each
season_days = np.sum(np.split(np.array(month_days),4),1)
# use cumsum to establish the indices separating the four seasons
season_breaks = np.cumsum(season_days)

def getGIDs():
    conn = pg.connect(pgConnParams1)
    cur = conn.cursor()
    sql = 'SELECT gid from solar_data ORDER BY gid ASC;'
    cur.execute(sql)    
    
    gids = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()
    
    return gids

def chunkList(inList,nbins):
    '''Determines the indices needed to slice a list in n bins'''
    # determine the number of elements that should be in each bin
    binsize = len(inList)/nbins
    # 
    binbreaks = list(np.array(range(1,nbins))*binsize)
    binbreaks.insert(0,0)
    binbreaks.append(None)
    bin_starts = binbreaks[0:-1]
    bin_ends = binbreaks[1:]
    bin_bnds = zip(bin_starts,bin_ends)
    print bin_bnds
    
    chunks = []
    for start,end in bin_bnds:
        chunk = inList[start:end]
        chunks.append(chunk)
    
    return chunks   

def iterCell(sqlDict, q, years):
    conn = pg.connect(pgConnParams1)
    cur = conn.cursor()
    #create SSC
    import  site
    site.addsitedir('/srv/data/transfer/shared/samTools/ssc/trunk/python')
    import sscapi
    ssc = sscapi.PySSC()
    #create list to stuff data in
    dataLoad = []
    for process, gidRange in sqlDict.iteritems():     
        #for each gid, run pvwatts for years between given range
        for year in years:                
            #using just in case program bombs late in the game...
            print 'Started Process: %s for Year: %s and Range: %s' % (process,year,gidRange)
            #gives us the weather file 
            sql = "SELECT gid, sam_weather_return(ST_Centroid(the_geom), '%s') FROM solar_data WHERE gid " % year + gidRange            
            cur.execute(sql)
            results = cur.fetchall()
            
            #run pvwatts with given info 
            for cell in results:
                gid, weather_file = cell[0], cell[1].split(':')[0]
                wfresults = wfreader(weather_file, year, ssc)
                if wfresults <> None:        # wfresults will only = None when weather_file is empty, in which case we don't have data anyway
                    # add year and gid to the dictionary
                    wfresults['gid'] = gid
                    wfresults['year'] = year
                    #load data into dict 
                    dataLoad.append(wfresults)

    #print 'Exited Process: %s for Year: %s and Range: %s' % (process,year,gidRange)
    del ssc
    del sscapi
    cur.close()
    conn.close()
    q.put(dataLoad)

def loadResults(queueData):
    #make a con to send data to DB
    conn = pg.connect(pgConnParams2)
    cur = conn.cursor()
    
    # drop all the tables so they can be recreated
    print 'dropping existing tables'
    sql = 'DROP TABLE IF EXISTS solar_om.all_seasons;' 
    cur.execute(sql)
    conn.commit()
    sql = 'DROP TABLE IF EXISTS solar_om.seasonal_averages;' 
    cur.execute(sql)
    conn.commit()
    sql = 'DROP TABLE IF EXISTS solar_om.all_high_ins;' 
    cur.execute(sql)
    conn.commit()
    sql = 'DROP TABLE IF EXISTS solar_om.high_ins_hours_aggregate;' 
    cur.execute(sql)
    conn.commit()
    sql = 'DROP TABLE IF EXISTS solar_om.errors;' 
    cur.execute(sql)
    conn.commit()    
    
    
    # create table to hold seasonal results for wind speed, temp, and humidity
    print 'creating new tables'
    sql = "CREATE TABLE solar_om.all_seasons (\
	gid integer,\
	year integer,\
	season character varying(6),\
	wspd_average_daily_max numeric,\
	tdry_average_daily_min numeric,\
	tdry_average_daily_max numeric,\
	rhum_average_daily_min numeric,\
	rhum_average_daily_max numeric\
             );"
    cur.execute(sql)
    conn.commit()
    
    # create table to hold annual results for high insolation days
    sql = "CREATE TABLE solar_om.all_high_ins (\
	gid integer,\
	year integer,\
	ghi_high_ins_hours integer\
             );"
    cur.execute(sql)
    conn.commit()    

    # create table to hold errors
    sql = "CREATE TABLE solar_om.errors ( \
            gid integer, \
            year integer, \
            errmsg text \
            );"
    cur.execute(sql)
    conn.commit()

    #iterate over list of results
    print 'inserting data to tables'
    for wfresults in queueData:
        if 'error' in wfresults.keys():
            # an error occurred and will be logged to the errors table
            sql = "INSERT INTO solar_om.errors (gid,year,errmsg) VALUES (%s,%s,'%s');" % (wfresults['gid'],wfresults['year'],wfresults['error'].replace("'","''"))
            cur.execute(sql)
            conn.commit()
        else:
            # the process completed successfuuly and can be added to the results                              
            # insert all counts of high insolation days by year
            sql = 'INSERT INTO solar_om.all_high_ins (gid,year,ghi_high_ins_hours) VALUES (%s,%s,%s);' % \
                                        (wfresults['gid'],wfresults['year'],wfresults['high_ins_hours'])
            cur.execute(sql)
            conn.commit()
            # insert all seasonal mins/maxes
            for season in seasons:
                season_data = wfresults[season]
                sql = "INSERT INTO solar_om.all_seasons \
                        (gid,year,season,\
                         wspd_average_daily_max,tdry_average_daily_min,tdry_average_daily_max, \
                         rhum_average_daily_min,rhum_average_daily_max)  \
                         VALUES (%s,%s,'%s',%s,%s,%s,%s,%s);" % \
                         (wfresults['gid'],wfresults['year'],season,
                          season_data['wspd_average_daily_max'],season_data['tdry_average_daily_min'],season_data['tdry_average_daily_max'],
                            season_data['rhum_average_daily_min'],season_data['rhum_average_daily_max'])
                cur.execute(sql)
                conn.commit()
        

    # aggregate the data    
    print 'aggregating results'
    sql = "CREATE TABLE solar_om.seasonal_averages AS \
            SELECT gid, season ,\
            avg(wspd_average_daily_max) as wspd_average_daily_max, \
            avg(tdry_average_daily_min) as tdry_average_daily_min, \
            avg(tdry_average_daily_max) as tdry_average_daily_max, \
            avg(rhum_average_daily_min) as rhum_average_daily_min, \
            avg(rhum_average_daily_max) as rhum_average_daily_max \
            FROM solar_om.all_seasons \
            GROUP BY gid, season;"
    cur.execute(sql)        
    conn.commit()
    
    sql = "CREATE TABLE solar_om.high_ins_hours_aggregate AS \
            SELECT gid,\
            avg(ghi_high_ins_hours) as mean_hours,\
            median(ghi_high_ins_hours) as median_hours,\
            stddev_samp(ghi_high_ins_hours) as std_hours\
            FROM solar_om.all_high_ins\
            GROUP BY gid;"
    cur.execute(sql)        
    conn.commit()    

    cur.close()
    conn.close()
    

def wfreader(weather_file, year, ssc):
    """

    """
    #Setup seasons here in case weather file fails...
    seasons = ['winter','spring','summer','fall']
    if weather_file != None: 
        ##depending on the weather file type, choose a source folder
        #perez weather file location
        if weather_file[0:5] == 'radwx':
            short_lat = int(weather_file[11:13])
            short_lon = int(weather_file[6:9])
            if short_lat % 2 > 0: short_lat-=1
            if short_lon % 2 > 0: short_lon-=1
#            short_lon+=2
#            print short_lon, short_lat
#             folder = '/srv/data/transfer/shared/samTools/perez_weather/9809_radwx/%s/' % (str(short_lon) + str(short_lat))
            folder = '/srv/data2/solar_weather_files/grid/9809_radwx/%s%s/' % (short_lon,short_lat)

        #tmy3 and tmy2 weather file location 
        else:
            # this should never occur
            folder = '/srv/data/transfer/shared/samTools/weather_files/'
    
        dat = ssc.data_create()
        
        leap_yrs = [2000, 2004, 2008]
        if year in leap_yrs:
            wname, ext = os.path.splitext(weather_file)
            weather_file = wname + '_sans_leap.tm2'
        ssc.data_set_string(dat, 'file_name', folder + weather_file)

        #run PV system simulation
        mod = ssc.module_create("wfreader")
        if ssc.module_exec(mod, dat) == 0:
            print 'Wfreader error'
            idx = 1
            msg = ssc.module_log(mod, 0)
            errors = []
            while (msg != None):
                print '\t: ' + msg
                errors.append(msg)
                msg = ssc.module_log(mod, idx)
                idx = idx + 1
            #if there was an error in the model, return the error msg from ssc
            ssc.module_free(mod)
            ssc.data_free(dat)
            return {'error': '%s' % errors}

        else:


            # WIND SPEED
            #get hourly data for that year
            wspd = np.array(ssc.data_get_array(dat,'wspd'))

            # split into 24 hour pieces, one row for each day
            wspd_daily = np.array(np.split(wspd,365))
            # get maximum for each day
            wspd_daily_max = np.max(wspd_daily,1)
            # roll the last 31 days (December) from the end of the array to beginning of the array
            wspd_daily_max_rolled = np.roll(wspd_daily_max,31)        
            # split by season and take average for each season
            wspd_seasonal_average_daily_max = dict(zip(seasons,[np.average(x) for x in np.split(wspd_daily_max_rolled,season_breaks[:-1])]))
            
            # TEMPERATURE (dry bulb) 
            #get hourly data for that year
            tdry = np.array(ssc.data_get_array(dat,'tdry'))
            # split into 24 hour pieces, one row for each day
            tdry_daily = np.array(np.split(tdry,365))
            # get maximum and minimum for each day
            tdry_daily_max = np.max(tdry_daily,1)
            tdry_daily_min = np.min(tdry_daily,1)
            # roll the last 31 days (December) from the end of the array to beginning of the array
            tdry_daily_max_rolled = np.roll(tdry_daily_max,31)
            tdry_daily_min_rolled = np.roll(tdry_daily_min,31)            
            # split by season and take average for each season
            tdry_seasonal_average_daily_min = dict(zip(seasons,[np.average(x) for x in np.split(tdry_daily_min_rolled,season_breaks[:-1])]))
            tdry_seasonal_average_daily_max = dict(zip(seasons,[np.average(x) for x in np.split(tdry_daily_max_rolled,season_breaks[:-1])]))
            
            # RELATIVE HUMIDITY 
            #get hourly data for that year
            rhum = np.array(ssc.data_get_array(dat,'rhum'))
            # split into 24 hour pieces, one row for each day
            rhum_daily = np.array(np.split(rhum,365))
            # get maximum and minimum for each day
            rhum_daily_max = np.max(rhum_daily,1)
            rhum_daily_min = np.min(rhum_daily,1)
            # roll the last 31 days (December) from the end of the array to beginning of the array
            rhum_daily_max_rolled = np.roll(rhum_daily_max,31)
            rhum_daily_min_rolled = np.roll(rhum_daily_min,31)            
            # split by season and take average for each season
            rhum_seasonal_average_daily_min = dict(zip(seasons,[np.average(x) for x in np.split(rhum_daily_min_rolled,season_breaks[:-1])]))
            rhum_seasonal_average_daily_max = dict(zip(seasons,[np.average(x) for x in np.split(rhum_daily_max_rolled,season_breaks[:-1])]))
                                                                
                                                                            
            # INSOLATION (NEED TO CODE THIS)
            # get hourly data for that year
            ghi_w = np.array(ssc.data_get_array(dat, "global"))
            # check for any days exceeding 1000
            high_insolation_hour_count = np.sum(ghi_w>1000)
         
            #Free the data, module
            ssc.module_free(mod)
            ssc.data_free(dat)
            
            results = {'high_ins_hours' : high_insolation_hour_count}
            for season in seasons:
                results[season] = {'wspd_average_daily_max' : wspd_seasonal_average_daily_max[season],
                                   'tdry_average_daily_max' : tdry_seasonal_average_daily_max[season],
                                   'tdry_average_daily_min' : tdry_seasonal_average_daily_min[season],
                                   'rhum_average_daily_max' : rhum_seasonal_average_daily_max[season],
                                   'rhum_average_daily_min' : rhum_seasonal_average_daily_min[season]}

            return results

    #no weather file to process, return 0s
    else:
        ssc.module_free(mod)
        ssc.data_free(dat)
        # return nothing -- nothing was passed in for a file name
        return None


if __name__ == '__main__':

    # initialize variables    
    years = range(1998,2010) # range of years of TMY data to process (1998-2009)
    processNum = 16 # number of cores to use for processing
    gids = getGIDs() # get all gids from the solar_data table
#    gids = range(61784,61884)
    process_chunks = chunkList(gids,processNum)    # split list of gids into N lists (where N = processNum)
    binList = []
    # for each chunk of gids, initialize a process number and the range of gids to process
    for p,chunk in enumerate(process_chunks):
        binDict = {p : 'BETWEEN %i AND %i' % (chunk[0],chunk[-1])} # get the first element and last element in each chunk
        binList.append(binDict)
        
    #create the queue
    q = JoinableQueue()

    #create the multiprocesses, pass the dictionary and queue and years
    myProcesses = []
    for sqlDict in binList:
        pr = Process(target=iterCell, args=(sqlDict,q,years))
        myProcesses.append(pr)

    #start the processes
    print 'extracting and summarizing data from weather files'
    for p in myProcesses:
        p.start()

    #runs until the queue is full
    while q.qsize() < processNum:
        pass

    #get data from queue
    queueData = []
    while q.qsize() > 0:
        data = q.get()
        queueData.extend(data)
        
    print 'loading results to postgres'
    loadResults(queueData)
    
    print 'done'