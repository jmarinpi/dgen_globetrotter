# -*- coding: utf-8 -*-
"""
Created on Thu Jan  8 13:14:32 2015

@author: mgleason
"""

def shared(f):
    f.shared = True
    return f  


def unshared(f):
    f.shared = False
    return f  