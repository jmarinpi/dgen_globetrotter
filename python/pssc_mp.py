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


class Task(object):

    def __init__(self, customer_id, generation_hourly, consumption_hourly, rate_json,
                 analysis_period=1., inflation_rate=0., degradation=(0.,),
                 return_values=('annual_energy_value',
                                'elec_cost_with_system_year1',
                                'elec_cost_without_system_year1')):  # , **kwargs):

        self.customer_id = customer_id
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

        # merge with self.customer_id
        r = {'customer_id': self.customer_id}
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


def pssc_mp(data):
    # @TODO: convert to loop
    """
        Distribute pssc processing across workers.

        :param data: Pandas dataframe with: customer_id, rate_json, generation_hourly, and consumption_hourly
        :return: list of dict; customer_id with output values
        """

    # get data
    # @TODO: rewrite for pandas dataframe in conjunction with load_test_data return value rewrite
    customer_id = -999  # @TODO: get from config
    rate_json = data['rate_json']
    generation_hourly = data['generation_hourly']
    consumption_hourly = data['consumption_hourly']

    # set number of iterations
    num_iterations = config.get('test_runs') or 1
    logger.debug('Found %s locations to process' % num_iterations)

    tasks = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()

    num_consumers = config.as_int('num_processors') or multiprocessing.cpu_count() - 1

    num_jobs = num_iterations

    consumers = [Consumer(tasks, results) for i in xrange(num_consumers)]
    for i, consumer in enumerate(consumers):
        logger.debug('Starting consumer %s (%i/%i)' % (consumer.name, i + 1, num_consumers))
        consumer.start()

    logger.info('Loading %i task(s)' % num_jobs)
    for i in xrange(num_jobs):
        tasks.put(Task(customer_id=customer_id, generation_hourly=generation_hourly,
                       consumption_hourly=consumption_hourly, rate_json=rate_json))

    logger.debug('Loading %i NULL job(s) to signal Consumers there are no more tasks' % num_consumers)
    for i in xrange(num_consumers):
        tasks.put(None)

    logger.info('Waiting for %i job(s) across %i worker(s) to finish' % (num_jobs, num_consumers))

    # get results as they are returned
    result_count = 0
    results_all = []
    while num_jobs:
        result = results.get()
        results_all.append(result)
        result_count += 1
        logger.info('Recieved results for %i tasks' % result_count)
        num_jobs -= 1

    logger.info('Completed %i of %i job(s)' % (result_count, num_iterations))

    return results_all


def test():
    # get test data
    test_data = load_test_data(test_id=True)
    logger.debug('Testing with: %s' % test_data)

    # run test
    t0 = time.time()
    test_results = pssc_mp(data=test_data)[0]
    t1 = time.time()

    # set expected results  # @TODO: get from config
    customer_id = -999
    urdb_rate_id = '539fc010ec4f024c27d8984b'
    cost_with_result = 806012
    cost_without_result = 838430

    # test results
    try:
        assert test_results['customer_id'] == customer_id
    except KeyError:
        logger.error('customer_id key missing from test results')
    except AssertionError:
        logger.error('Customer ID test failed with ID: %s and should have been' % (test_results['customer_id']), customer_id)

    try:
        assert test_results['urdb_rate_id'] == urdb_rate_id
    except KeyError:
        logger.error('urdb_rate_id key missing from test results')
    except AssertionError:
        logger.error('URDB Rate ID test failed with ID: %s and should have been %s' % (test_results['urdb_rate_id'], urdb_rate_id))

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
