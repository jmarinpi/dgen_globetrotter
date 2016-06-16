# -*- coding: utf-8 -*-
"""
Created on Tue Mar  3 13:46:40 2015

@author: mgleason
"""


class VersionError(Exception):
    pass

class UninstalledError(Exception):
    pass


def check_dependencies():
    
    f = open('requirements.txt', 'r')
    requirements = [l.replace('\n', '') for l in f.readlines()]
    f.close()
    for requirement in requirements:
        if '=' in requirement:
            package, version = requirement.split('=')
        else:
            package, version = [requirement, '']
    
        # try to lood the package
        try:
            installed = __import__(package)
        except:
             raise UninstalledError('%s is not installed.' % package)
        # check the version
        if version  <> '':
            if version <> installed.__version__:
                raise VersionError('Version for %s is not equal to %s' % (package, version))
                

