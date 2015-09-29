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
    # print(df)
    
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
            symbol_info['domain'].append({'key': '*'})
            # If we register the domain names, it seems that gdxdict expects us
            # to explicitly create a Set symbol for each domain. (Something that
            # can be done as an enhancement.)
            # symbol_info['domain'].append({'key': col})
    gdx.add_symbol(symbol_info)
    top_dim = gdx[symbol_name]
    
    def add_data(dim, data):
        """
        Appends data, the row of a csv file, to dim, the data structure holding
        a gdx symbol.
        
        Parameters:
            dim (gdxdict.gdxdim): top-level container for symbol data
            data (pds.Series): row of csv data, with index being the dimension 
                               name or 'value', and the corresponding value being
                               the dimension's set element or the parameter value,
                               respectively.
        """
        cur_dim = dim
        prev_value = None
        # each item in the series, except for the 'value', takes us farther into 
        # a tree of gdxdict.gdxdim objects. each level of the tree represents a 
        # dimension of the data. each actual value is at a leaf of the tree.        
        for i, value in data.iteritems():
            assert cur_dim is not None
            if prev_value is None:
                # initialize
                if 'name' not in cur_dim.info:
                    cur_dim.info['name'] = i
                else:
                    assert cur_dim.info['name'] == i
                prev_value = value
            elif i == 'value':
                # finalize, that is
                # register the value at the current level
                assert prev_value not in cur_dim
                cur_dim[prev_value] = value
                # this should be the last item in the series
                cur_dim = None
            else:
                # in the middle of the Series,
                # create or descend into the next level of the tree
                new_dim = None
                if prev_value in cur_dim:
                    # next level is already there, just grab it
                    new_dim = cur_dim[prev_value]
                    assert new_dim.info['name'] == i
                else:
                    # make a new level of the tree by creating a node for prev_value
                    # that points down to the next dimension
                    new_dim = gdxdict.gdxdim(cur_dim)
                    new_dim.info['name'] = i
                    cur_dim[prev_value] = new_dim
                prev_value = value
                cur_dim = new_dim
    
    # add each row to the gdx symbol
    for i, row in df.iterrows():
        add_data(top_dim, row)

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
    