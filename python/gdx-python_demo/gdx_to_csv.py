import gdxdict
import gdxx
import pandas as pds

import argparse
import os
import sys

def convert_gdx_to_csv(in_gdx, out_dir, gams_dir=None):
    # check inputs
    if not os.path.splitext(in_gdx)[1] == '.gdx':
        msg = "Input file '{}' is of unexpected type. Expected .gdx.".format(in_gdx)
        raise RuntimeError(msg)
    if not os.path.exists(in_gdx):
        raise RuntimeError("Input file '{}' does not exist.".format(in_gdx))

    gdx = gdxdict.gdxdict()
    gdx.read(in_gdx, gams_dir)
    
    for k in gdx:
        print("\nCreating data frame for {}.".format(k))
        info = gdx.getinfo(k)
        for key, value in info.items():
            print("{}: {}".format(key, value))

if __name__ == "__main__":

    # define and execute the command line interface
    parser = argparse.ArgumentParser(description='''Reads a gdx file into 
        pandas dataframes, and then writes them out as csv files.''')
    parser.add_argument('-i', '--in_gdx', help='''Input gdx file to be read
        and exported as one csv per symbol.''')
    parser.add_argument('-o', '--out_dir', default='./gdx_data/', 
        help='''Directory to which csvs are to be written.''')
    parser.add_argument('-g', '--gams_dir', help='''Path to GAMS installation 
        directory.''', default = None)
        
    args = parser.parse_args()
    
    try:
        # call the function that does all the work
        convert_gdx_to_csv(args.in_gdx, args.out_dir, args.gams_dir)
    except gdxx.GDX_error, err:
        print >>sys.stderr, "GDX Error: %s" % err.msg
        sys.exit(1)

    sys.exit(0)