__author__ = 'dhetting'

import configobj
import datetime
import logging
import os
import sys


# get configuration info
if len(sys.argv) > 1:
    config = configobj.ConfigObj(os.path.abspath(sys.argv[1]))
else:
    config = configobj.ConfigObj(os.path.abspath('config.ini'))

# get current date
cdate = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')


def initialize_logger(output_dir=os.getcwd(), level=None, extra_handlers=()):
    """
    Returns a logger object configured to display >= INFO to the screen and >= DEBUG to a log file.
    """
    # init logger
    logger = logging.getLogger('main')

    # create console handler and set level to info
    handler = logging.StreamHandler()

    # @TODO: introduced a bug somewhere that prevents (some?) printing to screen
    # get logger level from config if needed
    if level is None:
        level = config.get('logger_level') or 'INFO'

    # set level
    if level == 'DEBUG':
        handler.setLevel(logging.DEBUG)
    elif level == 'INFO':
        handler.setLevel(logging.INFO)

    # set formatter
    formatter = logging.Formatter('%(levelname)s - %(asctime)s -  %(funcName)s - %(lineno)d - Msg: "%(message)s"')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create error file handler and set level to error
    if 'file' in extra_handlers:
        handler = logging.FileHandler(os.path.join(output_dir, '%s_%s.log' % (config.filename[:-4], cdate)), 'w', encoding=None, delay='true')
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def make_conn(host=config.get('db_host'), dbname=config.get('db_name'), user=config.get('db_user'),
              password=config.get('db_password'), port=config.get('db_port'), lib='psycopg2', c_factory=None):
    """
    Returns a postgres connection with autocommit = True
    """

    if lib in ('psycopg2', 'p'):
        import psycopg2
        if c_factory is not None:
            import psycopg2.extras
            if c_factory in ('DictCursor',):
                conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port, cursor_factory=psycopg2.extras.DictCursor)
        else:
            conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    elif lib in ('sqlalchemy', 's'):
        from sqlalchemy import create_engine
        conn_str = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(user=user, password=password, host=host, port=port, db=dbname)
        eng = create_engine(conn_str, isolation_level="AUTOCOMMIT")
        conn = eng.connect()
        conn.execute('''SET search_path TO "{schema}", public;'''.format(schema=config.get('output_schema')))
        conn.execute('''SET ROLE "{role}";'''.format(role=config.get('role', config.get('output_schema') + '-writers')))
    else:
        conn = None
    return conn


# create logger
logger = initialize_logger(level='DEBUG')

# log config file
logger.debug('Config file: %s' % os.path.abspath(config.filename))

# SSC rate variables
param_types = {'analysis_period': 'SSC_NUMBER',
               'ur_ec_p7_t3_br': 'SSC_NUMBER',
               'ur_ec_p7_t3_sr': 'SSC_NUMBER',
               'ur_ec_p7_t3_ub': 'SSC_NUMBER',
               'ur_ec_p7_t4_br': 'SSC_NUMBER',
               'ur_ec_p7_t4_sr': 'SSC_NUMBER',
               'ur_ec_p7_t4_ub': 'SSC_NUMBER',
               'hourly_energy': 'SSC_ARRAY',
               'p_with_system': 'SSC_ARRAY',
               'e_load': 'SSC_ARRAY',
               'p_load': 'SSC_ARRAY',
               'inflation_rate': 'SSC_NUMBER',
               'degradation': 'SSC_ARRAY',
               'load_escalation': 'SSC_ARRAY',
               'rate_escalation': 'SSC_ARRAY',
               'ur_enable_net_metering': 'SSC_NUMBER',
               'ur_nm_yearend_sell_rate': 'SSC_NUMBER',
               'ur_monthly_fixed_charge': 'SSC_NUMBER',
               'ur_flat_buy_rate': 'SSC_NUMBER',
               'ur_flat_sell_rate': 'SSC_NUMBER',
               'ur_monthly_min_charge': 'SSC_NUMBER',
               'ur_annual_min_charge': 'SSC_NUMBER',
               'ur_ec_enable': 'SSC_NUMBER',
               'ur_ec_sched_weekday': 'SSC_MATRIX',
               'ur_ec_sched_weekend': 'SSC_MATRIX',
               'ur_ec_p1_t1_br': 'SSC_NUMBER',
               'ur_ec_p1_t1_sr': 'SSC_NUMBER',
               'ur_ec_p1_t1_ub': 'SSC_NUMBER',
               'ur_ec_p1_t2_br': 'SSC_NUMBER',
               'ur_ec_p1_t2_sr': 'SSC_NUMBER',
               'ur_ec_p1_t2_ub': 'SSC_NUMBER',
               'ur_ec_p1_t3_br': 'SSC_NUMBER',
               'ur_ec_p1_t3_sr': 'SSC_NUMBER',
               'ur_ec_p1_t3_ub': 'SSC_NUMBER',
               'ur_ec_p1_t4_br': 'SSC_NUMBER',
               'ur_ec_p1_t4_sr': 'SSC_NUMBER',
               'ur_ec_p1_t4_ub': 'SSC_NUMBER',
               'ur_ec_p1_t5_br': 'SSC_NUMBER',
               'ur_ec_p1_t5_sr': 'SSC_NUMBER',
               'ur_ec_p1_t5_ub': 'SSC_NUMBER',
               'ur_ec_p1_t6_br': 'SSC_NUMBER',
               'ur_ec_p1_t6_sr': 'SSC_NUMBER',
               'ur_ec_p1_t6_ub': 'SSC_NUMBER',
               'ur_ec_p2_t1_br': 'SSC_NUMBER',
               'ur_ec_p2_t1_sr': 'SSC_NUMBER',
               'ur_ec_p2_t1_ub': 'SSC_NUMBER',
               'ur_ec_p2_t2_br': 'SSC_NUMBER',
               'ur_ec_p2_t2_sr': 'SSC_NUMBER',
               'ur_ec_p2_t2_ub': 'SSC_NUMBER',
               'ur_ec_p2_t3_br': 'SSC_NUMBER',
               'ur_ec_p2_t3_sr': 'SSC_NUMBER',
               'ur_ec_p2_t3_ub': 'SSC_NUMBER',
               'ur_ec_p2_t4_br': 'SSC_NUMBER',
               'ur_ec_p2_t4_sr': 'SSC_NUMBER',
               'ur_ec_p2_t4_ub': 'SSC_NUMBER',
               'ur_ec_p2_t5_br': 'SSC_NUMBER',
               'ur_ec_p2_t5_sr': 'SSC_NUMBER',
               'ur_ec_p2_t5_ub': 'SSC_NUMBER',
               'ur_ec_p2_t6_br': 'SSC_NUMBER',
               'ur_ec_p2_t6_sr': 'SSC_NUMBER',
               'ur_ec_p2_t6_ub': 'SSC_NUMBER',
               'ur_ec_p3_t1_br': 'SSC_NUMBER',
               'ur_ec_p3_t1_sr': 'SSC_NUMBER',
               'ur_ec_p3_t1_ub': 'SSC_NUMBER',
               'ur_ec_p3_t2_br': 'SSC_NUMBER',
               'ur_ec_p3_t2_sr': 'SSC_NUMBER',
               'ur_ec_p3_t2_ub': 'SSC_NUMBER',
               'ur_ec_p3_t3_br': 'SSC_NUMBER',
               'ur_ec_p3_t3_sr': 'SSC_NUMBER',
               'ur_ec_p3_t3_ub': 'SSC_NUMBER',
               'ur_ec_p3_t4_br': 'SSC_NUMBER',
               'ur_ec_p3_t4_sr': 'SSC_NUMBER',
               'ur_ec_p3_t4_ub': 'SSC_NUMBER',
               'ur_ec_p3_t5_br': 'SSC_NUMBER',
               'ur_ec_p3_t5_sr': 'SSC_NUMBER',
               'ur_ec_p3_t5_ub': 'SSC_NUMBER',
               'ur_ec_p3_t6_br': 'SSC_NUMBER',
               'ur_ec_p3_t6_sr': 'SSC_NUMBER',
               'ur_ec_p3_t6_ub': 'SSC_NUMBER',
               'ur_ec_p4_t1_br': 'SSC_NUMBER',
               'ur_ec_p4_t1_sr': 'SSC_NUMBER',
               'ur_ec_p4_t1_ub': 'SSC_NUMBER',
               'ur_ec_p4_t2_br': 'SSC_NUMBER',
               'ur_ec_p4_t2_sr': 'SSC_NUMBER',
               'ur_ec_p4_t2_ub': 'SSC_NUMBER',
               'ur_ec_p4_t3_br': 'SSC_NUMBER',
               'ur_ec_p4_t3_sr': 'SSC_NUMBER',
               'ur_ec_p4_t3_ub': 'SSC_NUMBER',
               'ur_ec_p4_t4_br': 'SSC_NUMBER',
               'ur_ec_p4_t4_sr': 'SSC_NUMBER',
               'ur_ec_p4_t4_ub': 'SSC_NUMBER',
               'ur_ec_p4_t5_br': 'SSC_NUMBER',
               'ur_ec_p4_t5_sr': 'SSC_NUMBER',
               'ur_ec_p4_t5_ub': 'SSC_NUMBER',
               'ur_ec_p4_t6_br': 'SSC_NUMBER',
               'ur_ec_p4_t6_sr': 'SSC_NUMBER',
               'ur_ec_p4_t6_ub': 'SSC_NUMBER',
               'ur_ec_p5_t1_br': 'SSC_NUMBER',
               'ur_ec_p5_t1_sr': 'SSC_NUMBER',
               'ur_ec_p5_t1_ub': 'SSC_NUMBER',
               'ur_ec_p5_t2_br': 'SSC_NUMBER',
               'ur_ec_p5_t2_sr': 'SSC_NUMBER',
               'ur_ec_p5_t2_ub': 'SSC_NUMBER',
               'ur_ec_p5_t3_br': 'SSC_NUMBER',
               'ur_ec_p5_t3_sr': 'SSC_NUMBER',
               'ur_ec_p5_t3_ub': 'SSC_NUMBER',
               'ur_ec_p5_t4_br': 'SSC_NUMBER',
               'ur_ec_p5_t4_sr': 'SSC_NUMBER',
               'ur_ec_p5_t4_ub': 'SSC_NUMBER',
               'ur_ec_p5_t5_br': 'SSC_NUMBER',
               'ur_ec_p5_t5_sr': 'SSC_NUMBER',
               'ur_ec_p5_t5_ub': 'SSC_NUMBER',
               'ur_ec_p5_t6_br': 'SSC_NUMBER',
               'ur_ec_p5_t6_sr': 'SSC_NUMBER',
               'ur_ec_p5_t6_ub': 'SSC_NUMBER',
               'ur_ec_p6_t1_br': 'SSC_NUMBER',
               'ur_ec_p6_t1_sr': 'SSC_NUMBER',
               'ur_ec_p6_t1_ub': 'SSC_NUMBER',
               'ur_ec_p6_t2_br': 'SSC_NUMBER',
               'ur_ec_p6_t2_sr': 'SSC_NUMBER',
               'ur_ec_p6_t2_ub': 'SSC_NUMBER',
               'ur_ec_p6_t3_br': 'SSC_NUMBER',
               'ur_ec_p6_t3_sr': 'SSC_NUMBER',
               'ur_ec_p6_t3_ub': 'SSC_NUMBER',
               'ur_ec_p6_t4_br': 'SSC_NUMBER',
               'ur_ec_p6_t4_sr': 'SSC_NUMBER',
               'ur_ec_p6_t4_ub': 'SSC_NUMBER',
               'ur_ec_p6_t5_br': 'SSC_NUMBER',
               'ur_ec_p6_t5_sr': 'SSC_NUMBER',
               'ur_ec_p6_t5_ub': 'SSC_NUMBER',
               'ur_ec_p6_t6_br': 'SSC_NUMBER',
               'ur_ec_p6_t6_sr': 'SSC_NUMBER',
               'ur_ec_p6_t6_ub': 'SSC_NUMBER',
               'ur_ec_p7_t1_br': 'SSC_NUMBER',
               'ur_ec_p7_t1_sr': 'SSC_NUMBER',
               'ur_ec_p7_t1_ub': 'SSC_NUMBER',
               'ur_ec_p7_t2_br': 'SSC_NUMBER',
               'ur_ec_p7_t2_sr': 'SSC_NUMBER',
               'ur_ec_p7_t2_ub': 'SSC_NUMBER',
               'ur_ec_p7_t5_br': 'SSC_NUMBER',
               'ur_ec_p7_t5_sr': 'SSC_NUMBER',
               'ur_ec_p7_t5_ub': 'SSC_NUMBER',
               'ur_ec_p7_t6_br': 'SSC_NUMBER',
               'ur_ec_p7_t6_sr': 'SSC_NUMBER',
               'ur_ec_p7_t6_ub': 'SSC_NUMBER',
               'ur_ec_p8_t1_br': 'SSC_NUMBER',
               'ur_ec_p8_t1_sr': 'SSC_NUMBER',
               'ur_ec_p8_t1_ub': 'SSC_NUMBER',
               'ur_ec_p8_t2_br': 'SSC_NUMBER',
               'ur_ec_p8_t2_sr': 'SSC_NUMBER',
               'ur_ec_p8_t2_ub': 'SSC_NUMBER',
               'ur_ec_p8_t3_br': 'SSC_NUMBER',
               'ur_ec_p8_t3_sr': 'SSC_NUMBER',
               'ur_ec_p8_t3_ub': 'SSC_NUMBER',
               'ur_ec_p8_t4_br': 'SSC_NUMBER',
               'ur_ec_p8_t4_sr': 'SSC_NUMBER',
               'ur_ec_p8_t4_ub': 'SSC_NUMBER',
               'ur_ec_p8_t5_br': 'SSC_NUMBER',
               'ur_ec_p8_t5_sr': 'SSC_NUMBER',
               'ur_ec_p8_t5_ub': 'SSC_NUMBER',
               'ur_ec_p8_t6_br': 'SSC_NUMBER',
               'ur_ec_p8_t6_sr': 'SSC_NUMBER',
               'ur_ec_p8_t6_ub': 'SSC_NUMBER',
               'ur_ec_p9_t1_br': 'SSC_NUMBER',
               'ur_ec_p9_t1_sr': 'SSC_NUMBER',
               'ur_ec_p9_t1_ub': 'SSC_NUMBER',
               'ur_ec_p9_t2_br': 'SSC_NUMBER',
               'ur_ec_p9_t2_sr': 'SSC_NUMBER',
               'ur_ec_p9_t2_ub': 'SSC_NUMBER',
               'ur_ec_p9_t3_br': 'SSC_NUMBER',
               'ur_ec_p9_t3_sr': 'SSC_NUMBER',
               'ur_ec_p9_t3_ub': 'SSC_NUMBER',
               'ur_ec_p9_t4_br': 'SSC_NUMBER',
               'ur_ec_p9_t4_sr': 'SSC_NUMBER',
               'ur_ec_p9_t4_ub': 'SSC_NUMBER',
               'ur_ec_p9_t5_br': 'SSC_NUMBER',
               'ur_ec_p9_t5_sr': 'SSC_NUMBER',
               'ur_ec_p9_t5_ub': 'SSC_NUMBER',
               'ur_ec_p9_t6_br': 'SSC_NUMBER',
               'ur_ec_p9_t6_sr': 'SSC_NUMBER',
               'ur_ec_p9_t6_ub': 'SSC_NUMBER',
               'ur_ec_p10_t1_br': 'SSC_NUMBER',
               'ur_ec_p10_t1_sr': 'SSC_NUMBER',
               'ur_ec_p10_t1_ub': 'SSC_NUMBER',
               'ur_ec_p10_t2_br': 'SSC_NUMBER',
               'ur_ec_p10_t2_sr': 'SSC_NUMBER',
               'ur_ec_p10_t2_ub': 'SSC_NUMBER',
               'ur_ec_p10_t3_br': 'SSC_NUMBER',
               'ur_ec_p10_t3_sr': 'SSC_NUMBER',
               'ur_ec_p10_t3_ub': 'SSC_NUMBER',
               'ur_ec_p10_t4_br': 'SSC_NUMBER',
               'ur_ec_p10_t4_sr': 'SSC_NUMBER',
               'ur_ec_p10_t4_ub': 'SSC_NUMBER',
               'ur_ec_p10_t5_br': 'SSC_NUMBER',
               'ur_ec_p10_t5_sr': 'SSC_NUMBER',
               'ur_ec_p10_t5_ub': 'SSC_NUMBER',
               'ur_ec_p10_t6_br': 'SSC_NUMBER',
               'ur_ec_p10_t6_sr': 'SSC_NUMBER',
               'ur_ec_p10_t6_ub': 'SSC_NUMBER',
               'ur_ec_p11_t1_br': 'SSC_NUMBER',
               'ur_ec_p11_t1_sr': 'SSC_NUMBER',
               'ur_ec_p11_t1_ub': 'SSC_NUMBER',
               'ur_ec_p11_t2_br': 'SSC_NUMBER',
               'ur_ec_p11_t2_sr': 'SSC_NUMBER',
               'ur_ec_p11_t2_ub': 'SSC_NUMBER',
               'ur_ec_p11_t3_br': 'SSC_NUMBER',
               'ur_ec_p11_t3_sr': 'SSC_NUMBER',
               'ur_ec_p11_t3_ub': 'SSC_NUMBER',
               'ur_ec_p11_t4_br': 'SSC_NUMBER',
               'ur_ec_p11_t4_sr': 'SSC_NUMBER',
               'ur_ec_p11_t4_ub': 'SSC_NUMBER',
               'ur_ec_p11_t5_br': 'SSC_NUMBER',
               'ur_ec_p11_t5_sr': 'SSC_NUMBER',
               'ur_ec_p11_t5_ub': 'SSC_NUMBER',
               'ur_ec_p11_t6_br': 'SSC_NUMBER',
               'ur_ec_p11_t6_sr': 'SSC_NUMBER',
               'ur_ec_p11_t6_ub': 'SSC_NUMBER',
               'ur_ec_p12_t1_br': 'SSC_NUMBER',
               'ur_ec_p12_t1_sr': 'SSC_NUMBER',
               'ur_ec_p12_t1_ub': 'SSC_NUMBER',
               'ur_ec_p12_t2_br': 'SSC_NUMBER',
               'ur_ec_p12_t2_sr': 'SSC_NUMBER',
               'ur_ec_p12_t2_ub': 'SSC_NUMBER',
               'ur_ec_p12_t3_br': 'SSC_NUMBER',
               'ur_ec_p12_t3_sr': 'SSC_NUMBER',
               'ur_ec_p12_t3_ub': 'SSC_NUMBER',
               'ur_ec_p12_t4_br': 'SSC_NUMBER',
               'ur_ec_p12_t4_sr': 'SSC_NUMBER',
               'ur_ec_p12_t4_ub': 'SSC_NUMBER',
               'ur_ec_p12_t5_br': 'SSC_NUMBER',
               'ur_ec_p12_t5_sr': 'SSC_NUMBER',
               'ur_ec_p12_t5_ub': 'SSC_NUMBER',
               'ur_ec_p12_t6_br': 'SSC_NUMBER',
               'ur_ec_p12_t6_sr': 'SSC_NUMBER',
               'ur_ec_p12_t6_ub': 'SSC_NUMBER',
               'ur_dc_enable': 'SSC_NUMBER',
               'ur_dc_sched_weekday': 'SSC_MATRIX',
               'ur_dc_sched_weekend': 'SSC_MATRIX',
               'ur_dc_p1_t1_dc': 'SSC_NUMBER',
               'ur_dc_p1_t1_ub': 'SSC_NUMBER',
               'ur_dc_p1_t2_dc': 'SSC_NUMBER',
               'ur_dc_p1_t2_ub': 'SSC_NUMBER',
               'ur_dc_p1_t3_dc': 'SSC_NUMBER',
               'ur_dc_p1_t3_ub': 'SSC_NUMBER',
               'ur_dc_p1_t4_dc': 'SSC_NUMBER',
               'ur_dc_p1_t4_ub': 'SSC_NUMBER',
               'ur_dc_p1_t5_dc': 'SSC_NUMBER',
               'ur_dc_p1_t5_ub': 'SSC_NUMBER',
               'ur_dc_p1_t6_dc': 'SSC_NUMBER',
               'ur_dc_p1_t6_ub': 'SSC_NUMBER',
               'ur_dc_p2_t1_dc': 'SSC_NUMBER',
               'ur_dc_p2_t1_ub': 'SSC_NUMBER',
               'ur_dc_p2_t2_dc': 'SSC_NUMBER',
               'ur_dc_p2_t2_ub': 'SSC_NUMBER',
               'ur_dc_p2_t3_dc': 'SSC_NUMBER',
               'ur_dc_p2_t3_ub': 'SSC_NUMBER',
               'ur_dc_p2_t4_dc': 'SSC_NUMBER',
               'ur_dc_p2_t4_ub': 'SSC_NUMBER',
               'ur_dc_p2_t5_dc': 'SSC_NUMBER',
               'ur_dc_p2_t5_ub': 'SSC_NUMBER',
               'ur_dc_p2_t6_dc': 'SSC_NUMBER',
               'ur_dc_p2_t6_ub': 'SSC_NUMBER',
               'ur_dc_p3_t1_dc': 'SSC_NUMBER',
               'ur_dc_p3_t1_ub': 'SSC_NUMBER',
               'ur_dc_p3_t2_dc': 'SSC_NUMBER',
               'ur_dc_p3_t2_ub': 'SSC_NUMBER',
               'ur_dc_p3_t3_dc': 'SSC_NUMBER',
               'ur_dc_p3_t3_ub': 'SSC_NUMBER',
               'ur_dc_p3_t4_dc': 'SSC_NUMBER',
               'ur_dc_p3_t4_ub': 'SSC_NUMBER',
               'ur_dc_p3_t5_dc': 'SSC_NUMBER',
               'ur_dc_p3_t5_ub': 'SSC_NUMBER',
               'ur_dc_p3_t6_dc': 'SSC_NUMBER',
               'ur_dc_p3_t6_ub': 'SSC_NUMBER',
               'ur_dc_p4_t1_dc': 'SSC_NUMBER',
               'ur_dc_p4_t1_ub': 'SSC_NUMBER',
               'ur_dc_p4_t2_dc': 'SSC_NUMBER',
               'ur_dc_p4_t2_ub': 'SSC_NUMBER',
               'ur_dc_p4_t3_dc': 'SSC_NUMBER',
               'ur_dc_p4_t3_ub': 'SSC_NUMBER',
               'ur_dc_p4_t4_dc': 'SSC_NUMBER',
               'ur_dc_p4_t4_ub': 'SSC_NUMBER',
               'ur_dc_p4_t5_dc': 'SSC_NUMBER',
               'ur_dc_p4_t5_ub': 'SSC_NUMBER',
               'ur_dc_p4_t6_dc': 'SSC_NUMBER',
               'ur_dc_p4_t6_ub': 'SSC_NUMBER',
               'ur_dc_p5_t1_dc': 'SSC_NUMBER',
               'ur_dc_p5_t1_ub': 'SSC_NUMBER',
               'ur_dc_p5_t2_dc': 'SSC_NUMBER',
               'ur_dc_p5_t2_ub': 'SSC_NUMBER',
               'ur_dc_p5_t3_dc': 'SSC_NUMBER',
               'ur_dc_p5_t3_ub': 'SSC_NUMBER',
               'ur_dc_p5_t4_dc': 'SSC_NUMBER',
               'ur_dc_p5_t4_ub': 'SSC_NUMBER',
               'ur_dc_p5_t5_dc': 'SSC_NUMBER',
               'ur_dc_p5_t5_ub': 'SSC_NUMBER',
               'ur_dc_p5_t6_dc': 'SSC_NUMBER',
               'ur_dc_p5_t6_ub': 'SSC_NUMBER',
               'ur_dc_p6_t1_dc': 'SSC_NUMBER',
               'ur_dc_p6_t1_ub': 'SSC_NUMBER',
               'ur_dc_p6_t2_dc': 'SSC_NUMBER',
               'ur_dc_p6_t2_ub': 'SSC_NUMBER',
               'ur_dc_p6_t3_dc': 'SSC_NUMBER',
               'ur_dc_p6_t3_ub': 'SSC_NUMBER',
               'ur_dc_p6_t4_dc': 'SSC_NUMBER',
               'ur_dc_p6_t4_ub': 'SSC_NUMBER',
               'ur_dc_p6_t5_dc': 'SSC_NUMBER',
               'ur_dc_p6_t5_ub': 'SSC_NUMBER',
               'ur_dc_p6_t6_dc': 'SSC_NUMBER',
               'ur_dc_p6_t6_ub': 'SSC_NUMBER',
               'ur_dc_p7_t1_dc': 'SSC_NUMBER',
               'ur_dc_p7_t1_ub': 'SSC_NUMBER',
               'ur_dc_p7_t2_dc': 'SSC_NUMBER',
               'ur_dc_p7_t2_ub': 'SSC_NUMBER',
               'ur_dc_p7_t3_dc': 'SSC_NUMBER',
               'ur_dc_p7_t3_ub': 'SSC_NUMBER',
               'ur_dc_p7_t4_dc': 'SSC_NUMBER',
               'ur_dc_p7_t4_ub': 'SSC_NUMBER',
               'ur_dc_p7_t5_dc': 'SSC_NUMBER',
               'ur_dc_p7_t5_ub': 'SSC_NUMBER',
               'ur_dc_p7_t6_dc': 'SSC_NUMBER',
               'ur_dc_p7_t6_ub': 'SSC_NUMBER',
               'ur_dc_p8_t1_dc': 'SSC_NUMBER',
               'ur_dc_p8_t1_ub': 'SSC_NUMBER',
               'ur_dc_p8_t2_dc': 'SSC_NUMBER',
               'ur_dc_p8_t2_ub': 'SSC_NUMBER',
               'ur_dc_p8_t3_dc': 'SSC_NUMBER',
               'ur_dc_p8_t3_ub': 'SSC_NUMBER',
               'ur_dc_p8_t4_dc': 'SSC_NUMBER',
               'ur_dc_p8_t4_ub': 'SSC_NUMBER',
               'ur_dc_p8_t5_dc': 'SSC_NUMBER',
               'ur_dc_p8_t5_ub': 'SSC_NUMBER',
               'ur_dc_p8_t6_dc': 'SSC_NUMBER',
               'ur_dc_p8_t6_ub': 'SSC_NUMBER',
               'ur_dc_p9_t1_dc': 'SSC_NUMBER',
               'ur_dc_p9_t1_ub': 'SSC_NUMBER',
               'ur_dc_p9_t2_dc': 'SSC_NUMBER',
               'ur_dc_p9_t2_ub': 'SSC_NUMBER',
               'ur_dc_p9_t3_dc': 'SSC_NUMBER',
               'ur_dc_p9_t3_ub': 'SSC_NUMBER',
               'ur_dc_p9_t4_dc': 'SSC_NUMBER',
               'ur_dc_p9_t4_ub': 'SSC_NUMBER',
               'ur_dc_p9_t5_dc': 'SSC_NUMBER',
               'ur_dc_p9_t5_ub': 'SSC_NUMBER',
               'ur_dc_p9_t6_dc': 'SSC_NUMBER',
               'ur_dc_p9_t6_ub': 'SSC_NUMBER',
               'ur_dc_p10_t1_dc': 'SSC_NUMBER',
               'ur_dc_p10_t1_ub': 'SSC_NUMBER',
               'ur_dc_p10_t2_dc': 'SSC_NUMBER',
               'ur_dc_p10_t2_ub': 'SSC_NUMBER',
               'ur_dc_p10_t3_dc': 'SSC_NUMBER',
               'ur_dc_p10_t3_ub': 'SSC_NUMBER',
               'ur_dc_p10_t4_dc': 'SSC_NUMBER',
               'ur_dc_p10_t4_ub': 'SSC_NUMBER',
               'ur_dc_p10_t5_dc': 'SSC_NUMBER',
               'ur_dc_p10_t5_ub': 'SSC_NUMBER',
               'ur_dc_p10_t6_dc': 'SSC_NUMBER',
               'ur_dc_p10_t6_ub': 'SSC_NUMBER',
               'ur_dc_p11_t1_dc': 'SSC_NUMBER',
               'ur_dc_p11_t1_ub': 'SSC_NUMBER',
               'ur_dc_p11_t2_dc': 'SSC_NUMBER',
               'ur_dc_p11_t2_ub': 'SSC_NUMBER',
               'ur_dc_p11_t3_dc': 'SSC_NUMBER',
               'ur_dc_p11_t3_ub': 'SSC_NUMBER',
               'ur_dc_p11_t4_dc': 'SSC_NUMBER',
               'ur_dc_p11_t4_ub': 'SSC_NUMBER',
               'ur_dc_p11_t5_dc': 'SSC_NUMBER',
               'ur_dc_p11_t5_ub': 'SSC_NUMBER',
               'ur_dc_p11_t6_dc': 'SSC_NUMBER',
               'ur_dc_p11_t6_ub': 'SSC_NUMBER',
               'ur_dc_p12_t1_dc': 'SSC_NUMBER',
               'ur_dc_p12_t1_ub': 'SSC_NUMBER',
               'ur_dc_p12_t2_dc': 'SSC_NUMBER',
               'ur_dc_p12_t2_ub': 'SSC_NUMBER',
               'ur_dc_p12_t3_dc': 'SSC_NUMBER',
               'ur_dc_p12_t3_ub': 'SSC_NUMBER',
               'ur_dc_p12_t4_dc': 'SSC_NUMBER',
               'ur_dc_p12_t4_ub': 'SSC_NUMBER',
               'ur_dc_p12_t5_dc': 'SSC_NUMBER',
               'ur_dc_p12_t5_ub': 'SSC_NUMBER',
               'ur_dc_p12_t6_dc': 'SSC_NUMBER',
               'ur_dc_p12_t6_ub': 'SSC_NUMBER',
               'ur_dc_jan_t1_dc': 'SSC_NUMBER',
               'ur_dc_jan_t1_ub': 'SSC_NUMBER',
               'ur_dc_jan_t2_dc': 'SSC_NUMBER',
               'ur_dc_jan_t2_ub': 'SSC_NUMBER',
               'ur_dc_jan_t3_dc': 'SSC_NUMBER',
               'ur_dc_jan_t3_ub': 'SSC_NUMBER',
               'ur_dc_jan_t4_dc': 'SSC_NUMBER',
               'ur_dc_jan_t4_ub': 'SSC_NUMBER',
               'ur_dc_jan_t5_dc': 'SSC_NUMBER',
               'ur_dc_jan_t5_ub': 'SSC_NUMBER',
               'ur_dc_jan_t6_dc': 'SSC_NUMBER',
               'ur_dc_jan_t6_ub': 'SSC_NUMBER',
               'ur_dc_feb_t1_dc': 'SSC_NUMBER',
               'ur_dc_feb_t1_ub': 'SSC_NUMBER',
               'ur_dc_feb_t2_dc': 'SSC_NUMBER',
               'ur_dc_feb_t2_ub': 'SSC_NUMBER',
               'ur_dc_feb_t3_dc': 'SSC_NUMBER',
               'ur_dc_feb_t3_ub': 'SSC_NUMBER',
               'ur_dc_feb_t4_dc': 'SSC_NUMBER',
               'ur_dc_feb_t4_ub': 'SSC_NUMBER',
               'ur_dc_feb_t5_dc': 'SSC_NUMBER',
               'ur_dc_feb_t5_ub': 'SSC_NUMBER',
               'ur_dc_feb_t6_dc': 'SSC_NUMBER',
               'ur_dc_feb_t6_ub': 'SSC_NUMBER',
               'ur_dc_mar_t1_dc': 'SSC_NUMBER',
               'ur_dc_mar_t1_ub': 'SSC_NUMBER',
               'ur_dc_mar_t2_dc': 'SSC_NUMBER',
               'ur_dc_mar_t2_ub': 'SSC_NUMBER',
               'ur_dc_mar_t3_dc': 'SSC_NUMBER',
               'ur_dc_mar_t3_ub': 'SSC_NUMBER',
               'ur_dc_mar_t4_dc': 'SSC_NUMBER',
               'ur_dc_mar_t4_ub': 'SSC_NUMBER',
               'ur_dc_mar_t5_dc': 'SSC_NUMBER',
               'ur_dc_mar_t5_ub': 'SSC_NUMBER',
               'ur_dc_mar_t6_dc': 'SSC_NUMBER',
               'ur_dc_mar_t6_ub': 'SSC_NUMBER',
               'ur_dc_apr_t1_dc': 'SSC_NUMBER',
               'ur_dc_apr_t1_ub': 'SSC_NUMBER',
               'ur_dc_apr_t2_dc': 'SSC_NUMBER',
               'ur_dc_apr_t2_ub': 'SSC_NUMBER',
               'ur_dc_apr_t3_dc': 'SSC_NUMBER',
               'ur_dc_apr_t3_ub': 'SSC_NUMBER',
               'ur_dc_apr_t4_dc': 'SSC_NUMBER',
               'ur_dc_apr_t4_ub': 'SSC_NUMBER',
               'ur_dc_apr_t5_dc': 'SSC_NUMBER',
               'ur_dc_apr_t5_ub': 'SSC_NUMBER',
               'ur_dc_apr_t6_dc': 'SSC_NUMBER',
               'ur_dc_apr_t6_ub': 'SSC_NUMBER',
               'ur_dc_may_t1_dc': 'SSC_NUMBER',
               'ur_dc_may_t1_ub': 'SSC_NUMBER',
               'ur_dc_may_t2_dc': 'SSC_NUMBER',
               'ur_dc_may_t2_ub': 'SSC_NUMBER',
               'ur_dc_may_t3_dc': 'SSC_NUMBER',
               'ur_dc_may_t3_ub': 'SSC_NUMBER',
               'ur_dc_may_t4_dc': 'SSC_NUMBER',
               'ur_dc_may_t4_ub': 'SSC_NUMBER',
               'ur_dc_may_t5_dc': 'SSC_NUMBER',
               'ur_dc_may_t5_ub': 'SSC_NUMBER',
               'ur_dc_may_t6_dc': 'SSC_NUMBER',
               'ur_dc_may_t6_ub': 'SSC_NUMBER',
               'ur_dc_jun_t1_dc': 'SSC_NUMBER',
               'ur_dc_jun_t1_ub': 'SSC_NUMBER',
               'ur_dc_jun_t2_dc': 'SSC_NUMBER',
               'ur_dc_jun_t2_ub': 'SSC_NUMBER',
               'ur_dc_jun_t3_dc': 'SSC_NUMBER',
               'ur_dc_jun_t3_ub': 'SSC_NUMBER',
               'ur_dc_jun_t4_dc': 'SSC_NUMBER',
               'ur_dc_jun_t4_ub': 'SSC_NUMBER',
               'ur_dc_jun_t5_dc': 'SSC_NUMBER',
               'ur_dc_jun_t5_ub': 'SSC_NUMBER',
               'ur_dc_jun_t6_dc': 'SSC_NUMBER',
               'ur_dc_jun_t6_ub': 'SSC_NUMBER',
               'ur_dc_jul_t1_dc': 'SSC_NUMBER',
               'ur_dc_jul_t1_ub': 'SSC_NUMBER',
               'ur_dc_jul_t2_dc': 'SSC_NUMBER',
               'ur_dc_jul_t2_ub': 'SSC_NUMBER',
               'ur_dc_jul_t3_dc': 'SSC_NUMBER',
               'ur_dc_jul_t3_ub': 'SSC_NUMBER',
               'ur_dc_jul_t4_dc': 'SSC_NUMBER',
               'ur_dc_jul_t4_ub': 'SSC_NUMBER',
               'ur_dc_jul_t5_dc': 'SSC_NUMBER',
               'ur_dc_jul_t5_ub': 'SSC_NUMBER',
               'ur_dc_jul_t6_dc': 'SSC_NUMBER',
               'ur_dc_jul_t6_ub': 'SSC_NUMBER',
               'ur_dc_aug_t1_dc': 'SSC_NUMBER',
               'ur_dc_aug_t1_ub': 'SSC_NUMBER',
               'ur_dc_aug_t2_dc': 'SSC_NUMBER',
               'ur_dc_aug_t2_ub': 'SSC_NUMBER',
               'ur_dc_aug_t3_dc': 'SSC_NUMBER',
               'ur_dc_aug_t3_ub': 'SSC_NUMBER',
               'ur_dc_aug_t4_dc': 'SSC_NUMBER',
               'ur_dc_aug_t4_ub': 'SSC_NUMBER',
               'ur_dc_aug_t5_dc': 'SSC_NUMBER',
               'ur_dc_aug_t5_ub': 'SSC_NUMBER',
               'ur_dc_aug_t6_dc': 'SSC_NUMBER',
               'ur_dc_aug_t6_ub': 'SSC_NUMBER',
               'ur_dc_sep_t1_dc': 'SSC_NUMBER',
               'ur_dc_sep_t1_ub': 'SSC_NUMBER',
               'ur_dc_sep_t2_dc': 'SSC_NUMBER',
               'ur_dc_sep_t2_ub': 'SSC_NUMBER',
               'ur_dc_sep_t3_dc': 'SSC_NUMBER',
               'ur_dc_sep_t3_ub': 'SSC_NUMBER',
               'ur_dc_sep_t4_dc': 'SSC_NUMBER',
               'ur_dc_sep_t4_ub': 'SSC_NUMBER',
               'ur_dc_sep_t5_dc': 'SSC_NUMBER',
               'ur_dc_sep_t5_ub': 'SSC_NUMBER',
               'ur_dc_sep_t6_dc': 'SSC_NUMBER',
               'ur_dc_sep_t6_ub': 'SSC_NUMBER',
               'ur_dc_oct_t1_dc': 'SSC_NUMBER',
               'ur_dc_oct_t1_ub': 'SSC_NUMBER',
               'ur_dc_oct_t2_dc': 'SSC_NUMBER',
               'ur_dc_oct_t2_ub': 'SSC_NUMBER',
               'ur_dc_oct_t3_dc': 'SSC_NUMBER',
               'ur_dc_oct_t3_ub': 'SSC_NUMBER',
               'ur_dc_oct_t4_dc': 'SSC_NUMBER',
               'ur_dc_oct_t4_ub': 'SSC_NUMBER',
               'ur_dc_oct_t5_dc': 'SSC_NUMBER',
               'ur_dc_oct_t5_ub': 'SSC_NUMBER',
               'ur_dc_oct_t6_dc': 'SSC_NUMBER',
               'ur_dc_oct_t6_ub': 'SSC_NUMBER',
               'ur_dc_nov_t1_dc': 'SSC_NUMBER',
               'ur_dc_nov_t1_ub': 'SSC_NUMBER',
               'ur_dc_nov_t2_dc': 'SSC_NUMBER',
               'ur_dc_nov_t2_ub': 'SSC_NUMBER',
               'ur_dc_nov_t3_dc': 'SSC_NUMBER',
               'ur_dc_nov_t3_ub': 'SSC_NUMBER',
               'ur_dc_nov_t4_dc': 'SSC_NUMBER',
               'ur_dc_nov_t4_ub': 'SSC_NUMBER',
               'ur_dc_nov_t5_dc': 'SSC_NUMBER',
               'ur_dc_nov_t5_ub': 'SSC_NUMBER',
               'ur_dc_nov_t6_dc': 'SSC_NUMBER',
               'ur_dc_nov_t6_ub': 'SSC_NUMBER',
               'ur_dc_dec_t1_dc': 'SSC_NUMBER',
               'ur_dc_dec_t1_ub': 'SSC_NUMBER',
               'ur_dc_dec_t2_dc': 'SSC_NUMBER',
               'ur_dc_dec_t2_ub': 'SSC_NUMBER',
               'ur_dc_dec_t3_dc': 'SSC_NUMBER',
               'ur_dc_dec_t3_ub': 'SSC_NUMBER',
               'ur_dc_dec_t4_dc': 'SSC_NUMBER',
               'ur_dc_dec_t4_ub': 'SSC_NUMBER',
               'ur_dc_dec_t5_dc': 'SSC_NUMBER',
               'ur_dc_dec_t5_ub': 'SSC_NUMBER',
               'ur_dc_dec_t6_dc': 'SSC_NUMBER',
               'ur_dc_dec_t6_ub': 'SSC_NUMBER',
               'annual_energy_value': 'SSC_ARRAY',
               'annual_electric_load': 'SSC_ARRAY',
               'elec_cost_with_system': 'SSC_ARRAY',
               'elec_cost_without_system': 'SSC_ARRAY',
               'elec_cost_with_system_year1': 'SSC_NUMBER',
               'elec_cost_without_system_year1': 'SSC_NUMBER',
               'savings_year1': 'SSC_NUMBER',
               'year1_electric_load': 'SSC_NUMBER',
               'year1_hourly_e_tofromgrid': 'SSC_ARRAY',
               'year1_hourly_load': 'SSC_ARRAY',
               'year1_hourly_p_tofromgrid': 'SSC_ARRAY',
               'year1_hourly_p_system_to_load': 'SSC_ARRAY',
               'year1_hourly_salespurchases_with_system': 'SSC_ARRAY',
               'year1_hourly_salespurchases_without_system': 'SSC_ARRAY',
               'year1_hourly_dc_with_system': 'SSC_ARRAY',
               'year1_hourly_dc_without_system': 'SSC_ARRAY',
               'year1_hourly_ec_tou_schedule': 'SSC_ARRAY',
               'year1_hourly_dc_tou_schedule': 'SSC_ARRAY',
               'year1_monthly_dc_fixed_with_system': 'SSC_ARRAY',
               'year1_monthly_dc_tou_with_system': 'SSC_ARRAY',
               'year1_monthly_ec_charge_with_system': 'SSC_ARRAY',
               'year1_monthly_dc_fixed_without_system': 'SSC_ARRAY',
               'year1_monthly_dc_tou_without_system': 'SSC_ARRAY',
               'year1_monthly_ec_charge_without_system': 'SSC_ARRAY',
               'year1_monthly_load': 'SSC_ARRAY',
               'year1_monthly_electricity_to_grid': 'SSC_ARRAY',
               'year1_monthly_cumulative_excess_generation': 'SSC_ARRAY',
               'year1_monthly_salespurchases': 'SSC_ARRAY',
               'year1_monthly_salespurchases_wo_sys': 'SSC_ARRAY',
               'charge_dc_fixed_jan': 'SSC_ARRAY',
               'charge_dc_fixed_feb': 'SSC_ARRAY',
               'charge_dc_fixed_mar': 'SSC_ARRAY',
               'charge_dc_fixed_apr': 'SSC_ARRAY',
               'charge_dc_fixed_may': 'SSC_ARRAY',
               'charge_dc_fixed_jun': 'SSC_ARRAY',
               'charge_dc_fixed_jul': 'SSC_ARRAY',
               'charge_dc_fixed_aug': 'SSC_ARRAY',
               'charge_dc_fixed_sep': 'SSC_ARRAY',
               'charge_dc_fixed_oct': 'SSC_ARRAY',
               'charge_dc_fixed_nov': 'SSC_ARRAY',
               'charge_dc_fixed_dec': 'SSC_ARRAY',
               'charge_dc_tou_jan': 'SSC_ARRAY',
               'charge_dc_tou_feb': 'SSC_ARRAY',
               'charge_dc_tou_mar': 'SSC_ARRAY',
               'charge_dc_tou_apr': 'SSC_ARRAY',
               'charge_dc_tou_may': 'SSC_ARRAY',
               'charge_dc_tou_jun': 'SSC_ARRAY',
               'charge_dc_tou_jul': 'SSC_ARRAY',
               'charge_dc_tou_aug': 'SSC_ARRAY',
               'charge_dc_tou_sep': 'SSC_ARRAY',
               'charge_dc_tou_oct': 'SSC_ARRAY',
               'charge_dc_tou_nov': 'SSC_ARRAY',
               'charge_dc_tou_dec': 'SSC_ARRAY',
               'charge_ec_jan': 'SSC_ARRAY',
               'charge_ec_feb': 'SSC_ARRAY',
               'charge_ec_mar': 'SSC_ARRAY',
               'charge_ec_apr': 'SSC_ARRAY',
               'charge_ec_may': 'SSC_ARRAY',
               'charge_ec_jun': 'SSC_ARRAY',
               'charge_ec_jul': 'SSC_ARRAY',
               'charge_ec_aug': 'SSC_ARRAY',
               'charge_ec_sep': 'SSC_ARRAY',
               'charge_ec_oct': 'SSC_ARRAY',
               'charge_ec_nov': 'SSC_ARRAY',
               'charge_ec_dec': 'SSC_ARRAY'}
# @TODO: expand with remaining outputs
output_types = {'annual_energy_value': 'SSC_ARRAY',
                'elec_cost_with_system_year1': 'SSC_NUMBER',
                'elec_cost_without_system_year1': 'SSC_NUMBER'}

    # 'gh': 'SSC_ARRAY',
    #             'dn': 'SSC_ARRAY',
    #             'df': 'SSC_ARRAY',
    #             'tamb': 'SSC_ARRAY',
    #             'tdew': 'SSC_ARRAY',
    #             'wspd': 'SSC_ARRAY',
    #             'poa': 'SSC_ARRAY',
    #             'tpoa': 'SSC_ARRAY',
    #             'tcell': 'SSC_ARRAY',
    #             'dc': 'SSC_ARRAY',
    #             'ac': 'SSC_ARRAY',
    #             'hourly_energy': 'SSC_ARRAY',
    #             'shad_beam_factor': 'SSC_ARRAY',
    #             'sunup': 'SSC_ARRAY',
    #             'poa_monthly': 'SSC_ARRAY',
    #             'solrad_monthly': 'SSC_ARRAY',
    #             'dc_monthly': 'SSC_ARRAY',
    #             'ac_monthly': 'SSC_ARRAY',
    #             'monthly_energy': 'SSC_ARRAY',
    #             'solrad_annual': 'SSC_NUMBER',
    #             'ac_annual': 'SSC_NUMBER',
    #             'annual_energy': 'SSC_NUMBER',
    #             'location': 'SSC_STRING',
    #             'city': 'SSC_STRING',
    #             'state': 'SSC_STRING',
    #             'lat': 'SSC_STRING',
    #             'lon': 'SSC_STRING',
    #             'tz': 'SSC_STRING',
    #             'elev': 'SSC_STRING',}