from nose.tools import nottest
import subprocess as subp

import os
import shutil

def gams_dir():
    # gdxcc is really picky about this
    # crashes if not forward slashes
    # should put safeguards in for user-given paths
    # should also put in GAMS finder
    return 'C:/GAMS/win64/24.3'

def base_dir():
    return os.path.dirname(__file__)

@nottest
def test_dir():
    return os.path.join(base_dir(),'test_output')

@nottest
def test_data_dir():
    return os.path.join(test_dir(),'data')    

def setup_module():
    if os.path.exists(test_dir()):
        shutil.rmtree(test_dir())
    os.mkdir(test_dir())
    shutil.copytree(os.path.join(base_dir(),'data'),test_data_dir())
    
def test_csv_to_gdx():
    cmds = ['python', os.path.join(base_dir(),'csv_to_gdx.py'),
            '-i',os.path.join(test_data_dir(),'installed_capacity.csv'),
                 os.path.join(test_data_dir(),'annual_generation.csv'),
            '-o',os.path.join(test_dir(),'output.gdx'),
            '-g',gams_dir()]
    subp.call(cmds)
    
def test_gdx_to_csv():
    cmds = ['python', os.path.join(base_dir(),'gdx_to_csv.py'),
            '-i',os.path.join(test_data_dir(),'CONVqn.gdx'),
            '-o',os.path.join(test_dir(),'output_csvs'),
            '-g',gams_dir()]
    subp.call(cmds)