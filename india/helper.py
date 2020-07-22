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