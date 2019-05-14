# -*- coding: utf-8 -*-
"""
Module of accessory decorators, mainly for logging purposes.

"""

import time
from functools import wraps
        
class fn_timer(object):
    """Decorater class for profiling the run-time of functions."""
    def __init__(self, logger = None, verbose = True, tab_level = 0, prefix = ''):
        self.verbose = verbose
        self.tabs = '\t' * tab_level
        self.prefix = prefix
        self.logger = logger

    def __call__(self, f):
            @wraps(f)
            def function_timer(*args, **kwargs):
                t0 = time.time()
                result = f(*args, **kwargs)
                t1 = time.time()
                if self.verbose:
                    msg = '%s%s%s completed in: %.1f seconds' % (self.tabs, self.prefix, f.__name__, t1 - t0)
                    if self.logger is not None:
                        self.logger.info(msg)
                    else:
                        print msg
                return result
            return function_timer