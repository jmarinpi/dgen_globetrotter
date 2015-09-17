# -*- coding: utf-8 -*-
"""
Created on Thu Jan  8 13:14:32 2015

@author: mgleason
"""

import time
from functools import wraps

def shared(f):
    f.shared = True
    return f  


def unshared(f):
    f.shared = False
    return f  
    
    
class fid(object):
    
    def __init__(self, i):
        self.fid = i

    def __call__(self, f):
        f.fid = self.fid
        return f
        
        
class fn_timer(object):
    
    def __init__(self, verbose = True, tab_level = 0, prefix = ''):
        self.verbose = verbose
        self.tabs = '\t' * tab_level
        self.prefix = prefix

    def __call__(self, f):
            @wraps(f)
            def function_timer(*args, **kwargs):
                t0 = time.time()
                result = f(*args, **kwargs)
                t1 = time.time()
                if self.verbose:
                    msg = '%s%sCompleted in: %.1f seconds' % (self.tabs, self.prefix, t1 - t0)
                    print (msg)
                return result
            return function_timer        
