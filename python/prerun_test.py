# -*- coding: utf-8 -*-
class VersionError(Exception):
    pass


class UninstalledError(Exception):
    pass


def check_dependencies():
    #Python
    requirements = ['collections',
                    'colorama',
                    'colorlog',
                    'datetime',
                    'decorators',
                    'functools',
                    'getopt',
                    'glob',
                    'gzip',
                    'json',
                    'logging',
                    'matplotlib',
                    'multiprocessing',
                    'numpy',
                    'openpyxl=2.3.2',
                    'os',
                    'pandas',
                    'pickle',
                    'psutil',
                    'scoop=0.7.1',
                    'shutil',
                    'subprocess',
                    'sys',
                    'time']

    for requirement in requirements:
        if '=' in requirement:
            package, version = requirement.split('=')
        else:
            package, version = [requirement, '']

        # try to load the package
        try:
            installed = __import__(package)
        except:
            raise UninstalledError('%s is not installed.' % package)
        # check the version
        if version <> '':
            if version <> installed.__version__:
                raise VersionError(
                    'Version for %s is not equal to %s' % (package, version))