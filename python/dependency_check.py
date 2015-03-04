# -*- coding: utf-8 -*-
"""
Created on Tue Mar  3 13:46:40 2015

@author: mgleason
"""
packages = ['time',
            'os',
            'pandas',
            'psycopg2',
            'numpy',
            'scipy',
            'glob',
            'matplotlib',
            'collections',
            'subprocess',
            'datetime',
            'shutil',
            'sys',
            'getopt',
            'pickle',
            'multiprocessing',
            'select',
            'cStringIO',
            'logging',
            'colorlog',
            'colorama',
            'gzip',
            'psutil',
            'json',
            'openpyxl',
            'decorators']

error = False
for package in packages:
    try:
        installed = __import__(package)
    except ImportError, e:
        print 'Error: %s is not installed.' % package    
        error = True

if not(error):
    print 'Success. All Python dependencies are loaded.'
else:
    print 'Failure. One or more Python dependncies are missing.'
