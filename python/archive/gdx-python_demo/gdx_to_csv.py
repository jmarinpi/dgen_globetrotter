import gdxdict
import gdxx
import pandas as pds

import argparse
import os
import sys

def print_info(symbol_info):
    print("Info:")
    
    print("  name:                 {}".format(symbol_info['name']))
    print("  type:                 {}".format(symbol_info['typename']))
    print("  description: {}".format(symbol_info['description']))
    print("  order in gdx:         {}".format(symbol_info['number']))
    print("  number of dimensions: {}".format(symbol_info['dims']))
    print("  number of records:    {}".format(symbol_info['records']))
    print("  domain:")
    for d in symbol_info['domain']:
       print("    {}".format(d))    
    already_printed = ['name','typename','description','number','dims','records','domain']    
    for key, value in symbol_info.items():
        if key not in already_printed:
            print("  {}: {}".format(key, value))

def collect_data(data,entry,dim):
    assert isinstance(dim, gdxdict.gdxdim)
    for key, value in dim.items.items():
        if isinstance(value,gdxdict.gdxdim):
            collect_data(data,entry + [key],value)
        else:
            data.append(entry + [key, value])

def convert_gdx_to_csv(in_gdx, out_dir, gams_dir=None):
    # check inputs
    if not os.path.splitext(in_gdx)[1] == '.gdx':
        msg = "Input file '{}' is of unexpected type. Expected .gdx.".format(in_gdx)
        raise RuntimeError(msg)
    if not os.path.exists(in_gdx):
        raise RuntimeError("Input file '{}' does not exist.".format(in_gdx))
    if not os.path.exists(os.path.dirname(out_dir)):
        raise RuntimeError("Parent directory of output directory '{}' does not exist.".format(out_dir))

    gdx = gdxdict.gdxdict()
    gdx.read(in_gdx, gams_dir)
    
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
   
    for symbol_name in gdx:
        print("\nCreating data frame for {}.".format(symbol_name))
        symbol_info = gdx.getinfo(symbol_name)
        print_info(symbol_info)
        if symbol_info['records'] > 0:
            cols = [d['key'] for d in symbol_info["domain"]]
            cols.append('value')
            data = []
            entry = []
            collect_data(data,entry,gdx[symbol_name])
            df = pds.DataFrame(data = data, columns = cols)
            csv_path = os.path.join(out_dir, symbol_name + ".csv")
            if os.path.exists(csv_path):
                print("Overwriting '{}'".format(csv_path))
            df.to_csv(csv_path,index=False)
        else:
            print("{} has no records. Skipping.".format(symbol_name))

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
        convert_gdx_to_csv(args.in_gdx, os.path.realpath(args.out_dir), args.gams_dir)
    except gdxx.GDX_error, err:
        print >>sys.stderr, "GDX Error: %s" % err.msg
        sys.exit(1)

    sys.exit(0)