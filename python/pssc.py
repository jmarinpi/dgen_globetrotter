# -*- coding: utf-8 -*-
__author__ = 'dhetting'
"""
Wrapper for SAM utilityrate3 module.
"""
import pssc_utils
from sam.languages.python import sscapi


def utilityrate3(generation_hourly, consumption_hourly, rate_json,
                 analysis_period=1., inflation_rate=0., degradation=(0.,),
                 return_values=('annual_energy_value', 'elec_cost_with_system_year1', 'elec_cost_without_system_year1'),
                 logger = None):
    """
    Run SAM utilityrate3 function.

    :param generation_hourly:  list; hourly resource generation
    :param consumption_hourly: list; hourly energy usage
    :param rate_json:          dict; keys match expected ssc.Data set parameters
                                     available in gispgb.dav-gis.urdb_rates.urdb3_verified_rates_sam_data_20141202.sam_json
    :param analysis_period:    float; analysis length in years (defaults to 1.0)
    :param inflation_rate:     float; annual inflation rate (defaults to 0.0)
    :param degradation:        list of floats; degradation rate
    :param return_values:      list of strings; any of: 'annual_energy_value',
                                                        'elec_cost_with_system_year1',
                                                        'elec_cost_without_system_year1'
                                                defaults to ('annual_energy_value',
                                                             'elec_cost_with_system_year1',
                                                              'elec_cost_without_system_year1')

    :return:                   dict; return_values, e.g.: {energy_value: ssc.Data.energy_value,
                                                           elec_cost_with_system: ssc.Data.elec_cost_with_system,
                                                           elec_cost_without_system: ssc.Data.elec_cost_without_system}
    """

    # if no logger provided, create one:
    if logger is None:
        from pssc_utils import logger

    # define return container
    return_value = dict()

    # init data container
    ssc = sscapi.PySSC()
    dat = ssc.data_create()

    dat_set = {'SSC_NUMBER': ssc.data_set_number,
               'SSC_STRING': ssc.data_set_string,
               'SSC_ARRAY': ssc.data_set_array,
               'SSC_MATRIX': ssc.data_set_matrix}

    dat_get = {'SSC_NUMBER': ssc.data_get_number,
               'SSC_STRING': ssc.data_get_string,
               'SSC_ARRAY': ssc.data_get_array,
               'SSC_MATRIX': ssc.data_get_matrix}

    # add function-level defaults
    for x, y in (('analysis_period', analysis_period),
                 ('inflation_rate', inflation_rate),
                 ('degradation', degradation)):
        logger.debug('Checking for %s' % x)
        if not x in rate_json:
            logger.debug('Setting %(x)s to %(y)s' % dict(x=x, y=y))
            rate_json[x] = y

    # set rate and function-level paramater values
    for k, v in rate_json.iteritems():
        # temporary fix for incorrectly named keys in rates json
        if k == 'ec_enable':
            k = 'ur_ec_enable'
        if k == 'dc_enable':
            k = 'ur_dc_enable'
        # logger.debug('setting %s' % k)
        try:
            dat_set[pssc_utils.param_types[k]](dat, k, v)
        except KeyError:
            logger.error('No datatype defined for %(u)s in %(f)s' % dict(u=k, f=pssc_utils.__file__))
        except TypeError:
            logger.error('Incorrect datatype provided _or_ no datatype defined for %(u)s in %(f)s' % dict(u=k, f=pssc_utils.__file__))
        except:
            raise

    # set generation and load
    dat_set['SSC_ARRAY'](dat, 'hourly_energy', generation_hourly)
    dat_set['SSC_ARRAY'](dat, 'e_load', consumption_hourly)

    # create SAM utilityrate3 module object
    utilityrate = ssc.module_create('utilityrate3')

    # run the module, printing errors if they occur
    if ssc.module_exec(utilityrate, dat) == 0:
        idx = 1
        msg = ssc.module_log(utilityrate, 0)
        while msg is not None:
            logger.error(msg)
            msg = ssc.module_log(utilityrate, idx)
            idx += 1
    else:
        logger.info('UtilityRate v3 Simulation complete')

        # # collect return values
        # # @TODO: convert to map()
        for v in return_values:
            logger.debug('Collecting %s' % v)
            return_value[v] = dat_get[pssc_utils.output_types[v]](dat, v)

    # free the module
    logger.debug('Freeing utilityrate module')
    ssc.module_free(utilityrate)

    # free the data
    logger.debug('Freeing data')
    ssc.data_free(dat)

    # return calculated energy value
    return return_value

#
# def loop_calc_energy_value(l, generation_hourly, consumption_hourly, rates_json, q):
#     energy_values = []
#     for i in l:
#         energy_value = utilityrate3(generation_hourly, consumption_hourly, rates_json)
#         energy_values.append(energy_value)
#
#     q.put(energy_values)
#
#     return True
#
#
# def main():
#
#     # LOAD TEST DATA
#     import pandas as pd
#
#     rates_json = {}
#     with make_conn(c_factory='DictCursor') as conn:
#         with conn.cursor() as cur:
#             sql = 'SELECT * FROM {rates_table} \
#                    WHERE "urdb_rate_id" = %(rate_id)s;'.format(rates_table=config.get('rates_table'))
#             val = dict(rate_id=config.get('test_rate_id'))
#             cur.execute(sql, val)
#             if cur.rowcount is not None:
#                 rates_json = cur.fetchall()[0]['sam_json']
#             else:
#                 pass
#                 # @TODO: add logging
#
#     # set additional defaults not supplied by db rates column
#     rates_json['ur_enable_net_metering'] = 1
#     rates_json['ur_nm_yearend_sell_rate'] = 0.1
#
#     if rates_json.has_key('ec_enable'):
#         rates_json['ur_ec_enable'] = rates_json.pop('ec_enable')
#     if rates_json.has_key('dc_enable'):
#         rates_json['ur_dc_enable'] = rates_json.pop('dc_enable')
#
#     tdata = pd.read_csv(config.get('test_data'))
#     generation_hourly = list(tdata.hourly_energy)
#     consumption_hourly = list(tdata.e_load)
#
#     # RUN IN PARALLEL
#     t0 = time.time()
#     q = multiprocessing.JoinableQueue()
#     procs = []
#     total_iterations = 5000
#     cores = 2
#     split_iterations = np.array_split(np.arange(0,total_iterations), cores)
#     for ilist in split_iterations:
#         proc = multiprocessing.Process(target=loop_calc_energy_value, args=(list(ilist), generation_hourly, consumption_hourly, rates_json, q))
#         procs.append(proc)
#         proc.start()
#
#     for p in procs:
#         p.join()
#
#     results = [q.get() for p in procs]
#     q.close()
# #    q.join_thread()
#     print time.time() - t0
#
#     # # RUN IN SERIAL  (276.061496973!)
#     # t0 = time.time()
#     # for i in range(0, 1):
#     #     cfs = utilityrate3(generation_hourly, consumption_hourly, rates_json)
#     #     #print cfs
#     # print time.time() - t0
#
# main()
#
#     # expect these returns for urdb_rate_id: 539fc010ec4f024c27d8984b
#     # elec_cost_with_system_year1 = $806012
#     # elec_cost_without_system_year1 = $838430
