import gdxdict
import gdxx
import pandas as pds

import argparse
import os
import sys

def append_csv_to_gdx(ifile,gdx): 
    print("Processing '{}'.".format(ifile))
    
    # open file into pandas DataFrame
    df = pds.DataFrame.from_csv(ifile, index_col = None)
    print(df)
    
    # create an info object for this csv file as a symbol
    # assume:
    #   1. symbol name same as file name
    #   2. header is present with 'value' column clearly labelled
    #   3. all other columns are a dimension defined by a set
    symbol_name = os.path.splitext(os.path.basename(ifile))[0]
    symbol_info = {}
    symbol_info['name'] = symbol_name
    symbol_info['typename'] = 'Parameter'
    symbol_info['dims'] = len(df.columns) - 1
    symbol_info['records'] = len(df.index)
    symbol_info['domain'] = []
    for col in df.columns:
        if not col == 'value':
            symbol_info['domain'].append({'key': col})
    gdx.add_symbol(symbol_info)
    top_dim = gdx[symbol_name]

def convert_csv_to_gdx(input_files, output_file, gams_dir=None):

    # check inputs
    #   - input_files should be list of .csv and .txt
    #   - output_file should be a gdx, and its parent directory should exist
    for ifile in input_files:
        if not os.path.splitext(ifile)[1] in ['.csv','.txt']:
            msg = "Input file '{}' is of unexpected type. Expected .csv or .txt.".format(ifile)
            raise RuntimeError(msg)
    if not os.path.splitext(output_file)[1] == '.gdx':
        msg = "Output file '{}' is of unexpected type. Expected .gdx.".format(output_file)
        raise RuntimeError(msg) 
    
    # convert input_files into one list of csvs
    ifiles = []
    for ifile in input_files:
        if os.path.splitext(ifile)[1] == '.csv':
            ifiles.append(ifile)
        else:
            # must be .txt
            f = open(ifile, 'r')
            for line in f:
                if not line == '':
                    if os.path.splitext(line)[1] == '.csv':
                        ifiles.append(line)
                    else:
                        print("Skipping '{}' found in '{}'.".format(line,ifile))
            f.close()
    if len(ifiles) == 0:
        raise RuntimeError("Nothing to convert.")
    
    # create a blank gdx
    gdx = gdxdict.gdxdict()
    
    # add each csv as a symbol in the gdx
    for ifile in ifiles:
        append_csv_to_gdx(ifile,gdx)
    
    # write the gdx
    if os.path.isfile(output_file):
        print("Overwriting '{}'.".format(output_file))
        os.remove(output_file)
    gdx.write(output_file, gams_dir)        

if __name__ == "__main__":

    # define and execute the command line interface
    parser = argparse.ArgumentParser(description='''Accepts one or more input 
        csv files as input. Writes each csv as a separate symbol to an output 
        gdx.''')
    parser.add_argument('-i', '--input', nargs='+', help='''List one or more 
        .csv or .txt files. The latter are assumed to be a line-delimited list 
        of .csv files.''')
    parser.add_argument('-o', '--output', default='export.gdx', help='''Path 
        to the output gdx file. Will be overwritten if it already exists.''')
    parser.add_argument('-g', '--gams_dir', help='''Path to GAMS installation 
        directory.''', default = None)
        
    args = parser.parse_args()
    
    try:
        # call the function that does all the work
        convert_csv_to_gdx(args.input, args.output, args.gams_dir)
    except gdxx.GDX_error, err:
        print >>sys.stderr, "GDX Error: %s" % err.msg
        sys.exit(1)

    sys.exit(0)
    