# -*- coding: utf-8 -*-
__author__ = 'dhetting'
"""
Multiprocessing for pssc module.
"""

import time
import multiprocessing

import pandas as pd

import pssc

from pssc_utils import logger
from pssc_utils import make_conn
from pssc_utils import config


class Consumer(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)

        self.task_queue = task_queue
        self.result_queue = result_queue

    def __str__(self):
        return '\n'.join('%s: %s' % (k, v) for k, v in self.__dict__.iteritems())

    def run(self):
        # logger.info('%s started' % self.name)
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                self.task_queue.task_done()
                break
            answer = next_task()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        # logger.info('%s completed' % self.name)
        return


def run_utilityrate3(uid, generation_hourly, consumption_hourly, rate_json,
                 analysis_period=1., inflation_rate=0., degradation=(0.,),
                 return_values=('annual_energy_value',
                                'elec_cost_with_system_year1',
                                'elec_cost_without_system_year1')):
                                    
    result = pssc.utilityrate3(generation_hourly = generation_hourly,
                                               consumption_hourly = consumption_hourly,
                                               rate_json = rate_json,
                                               analysis_period = analysis_period,
                                               inflation_rate = inflation_rate,
                                               degradation = degradation,
                                               return_values = return_values)
    result['uid'] = uid

    return result

    

class Task(object):

    def __init__(self, uid, generation_hourly, consumption_hourly, rate_json,
                 analysis_period=1., inflation_rate=0., degradation=(0.,),
                 return_values=('annual_energy_value',
                                'elec_cost_with_system_year1',
                                'elec_cost_without_system_year1')):  # , **kwargs):

        self.uid = uid
        self.generation_hourly = generation_hourly
        self.consumption_hourly = consumption_hourly
        self.rate_json = rate_json
        self.analysis_period = analysis_period
        self.inflation_rate = inflation_rate
        self.degradation = degradation
        self.return_values = return_values
        # self.__dict__.update(**kwargs)

    def __str__(self):
        return '\n'.join('%s: %s' % (k, v) for k, v in self.__dict__.iteritems())

    def __call__(self):

        # run pssc.utilityrates3 hotness
        self.utilityrates3 = pssc.utilityrate3(generation_hourly=self.generation_hourly,
                                               consumption_hourly=self.consumption_hourly,
                                               rate_json=self.rate_json,
                                               analysis_period=self.analysis_period,
                                               inflation_rate=self.inflation_rate,
                                               degradation=self.degradation,
                                               return_values=self.return_values)

        # merge with self.uid
        r = {'uid': self.uid}
        r.update(self.utilityrates3)

        # return value
        return r

    def to_csv(self, x, dest):
        """
        Save variable x to database
        """

        raise NotImplementedError

    def to_db(self, x, dest):
        """
        Save variable x to database
        """

        raise NotImplementedError

    def to_hdf(self, x, dest):
        """
        Save variable x to hdf
        """

        # x.to_hdf(dest, 'table', append=True)
        raise NotImplementedError


def load_test_data(test_id=True):
    rates_json = {}
    with make_conn(c_factory='DictCursor') as conn:
        with conn.cursor() as cur:
            rates_schema = config.get('rates_schema')
            rates_table = config.get('rates_table')
            sql = 'SELECT * FROM "{s}"."{t}"'.format(s=rates_schema, t=rates_table)
            val = dict()
            if test_id is True:
                t_id = config.get('test_rate_id')
                if t_id is not None:
                    sql += ' WHERE "urdb_rate_id" = %(i)s;'
                    val = dict(i=t_id)
            cur.execute(sql, val)
            if cur.rowcount is not None:
                rates_json = cur.fetchall()[0]['sam_json']
            else:
                msg = 'No records returned'
                if test_id is True:
                    msg += ' for urdb_rate_id: %s' % test_id
                logger.error(msg)

    # set additional defaults not supplied by db rates column
    if 'ur_enable_net_metering' not in rates_json:
        rates_json['ur_enable_net_metering'] = 1  # @TODO: get from config
    if 'ur_nm_yearend_sell_rate' not in rates_json:
        rates_json['ur_nm_yearend_sell_rate'] = 0.1  # @TODO: get from config

    if 'ec_enable' in rates_json:
        rates_json['ur_ec_enable'] = rates_json.pop('ec_enable')
    if 'dc_enable' in rates_json:
        rates_json['ur_dc_enable'] = rates_json.pop('dc_enable')

    tdata = pd.read_csv(config.get('test_data'))
    generation_hourly = list(tdata.hourly_energy)
    consumption_hourly = list(tdata.e_load)

    return {'rate_json': rates_json, 'generation_hourly': generation_hourly, 'consumption_hourly': consumption_hourly}  # @TODO: change to pandas dataframe




def create_consumers(num_consumers):

    tasks = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()

    consumers = [Consumer(tasks, results) for i in xrange(num_consumers)]
    for i, consumer in enumerate(consumers):
#        logger.debug('Starting consumer %s (%i/%i)' % (consumer.name, i + 1, num_consumers))
        consumer.start()    
    
    return consumers, tasks, results

    
def run_pssc(data, consumers, tasks, results):

    # set number of iterations
    num_jobs = data.shape[0] 
    num_consumers = len(consumers)

    for i in xrange(num_jobs):
        # get data
        # @TODO: rewrite for pandas dataframe in conjunction with load_test_data return value rewrite
        uid = data['uid'][i]  # @TODO: get from config
        rate_json = data['rate_json'][i]
        generation_hourly = data['generation_hourly'][i]
        consumption_hourly = data['consumption_hourly'][i]   
        data.drop(i, inplace = True)
        
        tasks.put(Task(uid=uid, generation_hourly=generation_hourly,
                       consumption_hourly=consumption_hourly, rate_json=rate_json,
                       analysis_period=1., inflation_rate=0., degradation=(0.,),
                 return_values=('elec_cost_with_system_year1', 'elec_cost_without_system_year1')))

#    logger.debug('Loading %i NULL job(s) to signal Consumers there are no more tasks' % num_consumers)
    for i in xrange(num_consumers):
        tasks.put(None)

#    logger.info('Waiting for %i job(s) across %i worker(s) to finish' % (num_jobs, num_consumers))

    # get results as they are returned
    result_count = 0
    results_all = []
    while num_jobs:
        result = results.get()
        results_all.append(result)
        result_count += 1
#        logger.info('Recieved results for %i tasks' % result_count)
        num_jobs -= 1

#    logger.info('Completed %i of %i job(s)' % (result_count, num_iterations))

    # delete consumer objects to free memory
    del consumers

    # convert results list to pandas a dataframe
    results_df = pd.DataFrame.from_dict(results_all)
    # round costs to 2 decimal places (i.e., pennies)
    results_df['elec_cost_with_system_year1'] = results_df['elec_cost_with_system_year1'].round(2)
    results_df['elec_cost_without_system_year1'] = results_df['elec_cost_without_system_year1'].round(2)
    
    return results_df

import multiprocessing

def pssc_mp(data, num_consumers):
    # @TODO: convert to loop
    """
        Distribute pssc processing across workers.

        :param data: Pandas dataframe with: 
                uid, rate_json, generation_hourly, and consumption_hourly
        :return: list of dict; uid with output values
        """



    # set number of iterations
    num_iterations = data.shape[0]
#    logger.debug('Found %s locations to process' % num_iterations)

    ########################################################################
    pool = multiprocessing.Pool(processes = num_consumers) 
    result_list = []
    
#    for i in l:
#        res = pool.apply_async(run_utilityrate3, (L, ModelSystem))
#        result_list.append(res)    
#
#    for i, result in enumerate(result_list):        
#        result_return = result.get()     
    ########################################################################
    
#    tasks = multiprocessing.JoinableQueue()
#    results = multiprocessing.Queue()

    num_jobs = num_iterations

#    consumers = [Consumer(tasks, results) for i in xrange(num_consumers)]
#    for i, consumer in enumerate(consumers):
##        logger.debug('Starting consumer %s (%i/%i)' % (consumer.name, i + 1, num_consumers))
#        consumer.start()

#    logger.info('Loading %i task(s)' % num_jobs)
    for i in xrange(num_jobs):
        # get data
        # @TODO: rewrite for pandas dataframe in conjunction with load_test_data return value rewrite
        uid = data['uid'][i]  # @TODO: get from config
        rate_json = data['rate_json'][i]
        generation_hourly = data['generation_hourly'][i]
        consumption_hourly = data['consumption_hourly'][i]   
        data.drop(i, inplace = True)
        
        res = pool.apply_async(run_utilityrate3, (uid, generation_hourly,consumption_hourly, rate_json, 1., 0., (0.,), 
                                                  ('elec_cost_with_system_year1', 'elec_cost_without_system_year1')))
        result_list.append(res)   
        
#        tasks.put(Task(uid=uid, generation_hourly=generation_hourly,
#                       consumption_hourly=consumption_hourly, rate_json=rate_json,
#                       analysis_period=1., inflation_rate=0., degradation=(0.,),
#                 return_values=('elec_cost_with_system_year1', 'elec_cost_without_system_year1')))

#    logger.debug('Loading %i NULL job(s) to signal Consumers there are no more tasks' % num_consumers)
#    for i in xrange(num_consumers):
#        tasks.put(None)

#    logger.info('Waiting for %i job(s) across %i worker(s) to finish' % (num_jobs, num_consumers))

    # get results as they are returned
#    result_count = 0
    results_all = []
    for i, result in enumerate(result_list):        
        result_return = result.get()     
        results_all.append(result_return)
        
#    while num_jobs:
#        result = results.get()
#        results_all.append(result)
#        result_count += 1
##        logger.info('Recieved results for %i tasks' % result_count)
#        num_jobs -= 1

#    logger.info('Completed %i of %i job(s)' % (result_count, num_iterations))

    # delete consumer objects to free memory
#    del consumers

    # convert results list to pandas a dataframe
    results_df = pd.DataFrame.from_dict(results_all)
    # round costs to 2 decimal places (i.e., pennies)
    results_df['elec_cost_with_system_year1'] = results_df['elec_cost_with_system_year1'].round(2)
    results_df['elec_cost_without_system_year1'] = results_df['elec_cost_without_system_year1'].round(2)
    
    return results_df


def test():
    # get test data
    test_data = load_test_data(test_id=True)
    # convert this into a pandas data frame with 50 rows
    test_df = pd.DataFrame.from_dict([test_data]*50)
    # add a customer id field
    test_df['uid'] = -999
    logger.debug('Testing with: %s' % test_data)

    # get/set the number of processors
    num_consumers = config.as_int('num_processors') or multiprocessing.cpu_count() - 1


    # run test
    t0 = time.time()
    test_results_df = pssc_mp(data=test_df, num_consumers=num_consumers)
    # extract just a single row
    test_results = test_results_df.ix[0]
    t1 = time.time()

    # set expected results  # @TODO: get from config
    uid = -999
    urdb_rate_id = '539fc010ec4f024c27d8984b'
    cost_with_result = 806011
    cost_without_result = 838430

    # test results
    try:
        assert test_results['uid'] == uid
    except KeyError:
        logger.error('uid key missing from test results')
    except AssertionError:
        logger.error('UID test failed with ID: %s and should have been' % (test_results['uid']), uid)

# Note to Dylan: test_results is never attributed with urdb_rate_id so this test will always fail
#    try:
#        assert test_results['urdb_rate_id'] == urdb_rate_id
#    except KeyError:
#        logger.error('urdb_rate_id key missing from test results')
#    except AssertionError:
#        logger.error('URDB Rate ID test failed with ID: %s and should have been %s' % (test_results['urdb_rate_id'], urdb_rate_id))
    

    try:
        assert int(test_results['elec_cost_with_system_year1']) == cost_with_result
    except KeyError:
        logger.error('elec_cost_with_system_year1 key missing from test results')
    except AssertionError:
        logger.error('Electric Cost with System Year 1 test failed with value: ${r:,G} and should have been ${v:,G}'.format(v=cost_with_result, r=test_results['elec_cost_with_system_year1']))

    try:
        assert int(test_results['elec_cost_without_system_year1']) == cost_without_result
    except KeyError:
        logger.error('elec_cost_with_system_year1 key missing from test results')
    except AssertionError:
        logger.error('Electric Cost without System Year 1 test failed with value: ${r:,G} and should have been ${v:,G}'.format(v=cost_without_result, r=test_results['elec_cost_without_system_year1']))

    logger.info('Completed in {:.5} seconds'.format(t1 - t0))
    print('Completed in {:.5} seconds'.format(t1 - t0))

if __name__ == '__main__':
    test()
