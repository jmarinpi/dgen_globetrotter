#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 20 13:30:11 2020

@author: skoebric
"""
import unicodedata

def sanitize_string(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    s = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    s = s.lower()
    s = s.replace(' ','_')
    s = s.replace('/','_')
    s = s.replace('&','')
    s = s.replace('(','_')
    s = s.replace(')','_')
    s = s.replace('____','_')
    s = s.replace('___','_')
    s = s.replace('__','_')
    if s[0] == '_': s = s[1:]
    if s[-1] == '_': s = s[:-1]
    return s


def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

import pandas as pd
from cfuzzyset import cFuzzySet as FuzzySet
def fuzzy_address_matcher(fuzzy_list, clean_list, thresh=0.5):

    if isinstance(fuzzy_list, pd.Series):
        fuzzy_list = fuzzy_list.tolist()
    
    if isinstance(clean_list, pd.Series):
        clean_list = clean_list.unique().tolist()

    index = FuzzySet()
    
    for c in clean_list:
        index.add(c)
    
    out_list = []
    for f in fuzzy_list:
        result = index.get(f)
        out_list.append(result[0][1])
    
    return out_list