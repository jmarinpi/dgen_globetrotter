# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 12:09:03 2015

@author: mgleason
"""

import openpyxl as xl
from cStringIO import StringIO
import numpy as np
import pandas as pd
import datetime
#import excel_functions as xl_funcs


class ExcelError(Exception):
    pass


class FancyNamedRange(object):
    
    def __init__(self, workbook, range_name):
        
        self.base = self.__base__(workbook, range_name)
        
        self.worksheets = self.__worksheets__()
        
        self.count_worksheets = self.__count_destination_components__(self.worksheets)
        if self.count_worksheets > 1:
            raise NotImplementedError("Named Ranges spanning multiple worksheets are not currently supported")
            
        self.worksheet = self.worksheets[0]            
            
        self.cell_ranges = self.__cell_ranges__()
        
        self.count_cell_ranges = self.__count_destination_components__(self.cell_ranges)
        if self.count_cell_ranges > 1:
            raise NotImplementedError("Named Ranges spanning multiple, non-contiguous cell ranges  are not currently supported")
        
        self.cell_range = self.cell_ranges[0]        
        
        self.topleft = self.__topleft__()
        self.bottomright = self.__bottomright__()
        
        self.cells = self.__cells__()
        
        self.cell_array = self.__cell_array__()
        
        self.rec_array = self.__rec_array__()
        
        self.data_frame = self.__data_frame__()
    

    def __colnames_included__(self):
        self.rec_array = self.__rec_array__(colnames_included = True)
        self.data_frame = self.__data_frame__(colnames_included = True)
    
    def __melt__(self):
        self.__colnames_included__()
        self.data_frame = pd.melt(self.data_frame, self.data_frame.columns[0])

    def __transpose_values__(self):
        
        self.cell_array = self.cell_array.T
        self.rec_array = self.__rec_array__()
        self.data_frame = self.__data_frame__()

    def __base__(self, workbook, range_name):
        
        # get the named range object
        base = workbook.get_named_range(range_name)
        
        # raise an error if the named range doesn't exist
        if base == None:
            raise ExcelError('%s named range does not exist.' % range_name)    
        
        return base    
    
    def __worksheets__(self):
        
        worksheets =  list(np.array(self.base.destinations)[:,0])
        
        return worksheets
        
    def __cell_ranges__(self):
        
        cell_ranges = list(np.array(self.base.destinations)[:,1])
        
        return cell_ranges
        
    
    def __count_destination_components__(self, destination_component):
        
        count = len(list(set(destination_component)))
        
        return count
    
    
    def __topleft__(self):
        
        if ':' in self.cell_ranges[0]:
            coordinates = self.cell_ranges[0].split(':')[0]
        else:
            coordinates = self.cell_ranges[0]
            
        return coordinates
        
    def __bottomright__(self):
        
        if ':' in self.cell_ranges[0]:
            coordinates = self.cell_ranges[0].split(':')[1]
        else:
            coordinates = self.cell_ranges[0]
        
        return coordinates
    
    def contents_to_array(self):
        pass
    
    
    def __columns__(self):
        
        self.topleft.split('$')
        
    def __cells__(self):
        
        cells = self.worksheet.range(self.cell_range)
        
        return cells
    
    def __cell_array__(self):
        
        cell_array = np.array(self.cells)
        if cell_array.shape == ():
            cell_array = cell_array.reshape((1,1))
        
        return cell_array
        
    def __cell_value__(self, cell, floats = True):
        
        if floats == True and cell.data_type == 'n' and type(cell.value) <> datetime.datetime:
            cell_value = float(cell.value)
        else:
            cell_value = cell.value
        
        if cell_value is None:
            cell_value = np.nan
        
        return cell_value
    
    def __rec_array__(self, colnames_included = False):
        
        cell_values = np.vectorize(self.__cell_value__)
        
        if colnames_included == True:
            i_begin = 1
        else:
            i_begin = 0
        
        cols = []
        for j in range(self.cell_array.shape[1]):
            col = cell_values(self.cell_array[i_begin:, j])
            if col.dtype.kind not in ('S', 'b', 'U', 'O') and np.all(col.astype('int') == col):
                col = col.astype('int')
            cols.append(col)
        
        rec_array = np.rec.fromarrays(cols)
        if colnames_included == True:
            names = cell_values(self.cell_array[0, :], floats = False)
            rec_array.dtype.names = map(str, list(names))
        
        return rec_array
    
    def __data_frame__(self, colnames_included = False):
        
        df = pd.DataFrame(self.rec_array)
        if colnames_included == False:
            ncols = df.shape[1]
            df.columns = range(0, ncols)
        
        return df
        
    def first_value(self):
        
        first_value = self.data_frame.ix[0][0]

        return first_value
        

        
    
    
    def to_stringIO(self, transpose = False, columns = None, index = False, header = False):
        
        s = StringIO()
        
        if columns == None:
            columns = self.data_frame.columns
        
        if transpose:
            out_df = self.data_frame[columns].T
        else:
            out_df = self.data_frame[columns]
            
        out_df.to_csv(s, delimiter = ',', index = index, header = header)
        
        s.seek(0)
        
        return s
        
    def to_postgres(self, connection, cursor, schema, table, transpose = False, columns = None, create = False, overwrite = True):
        
        sql_dict = {'schema': schema, 'table': table}
        
        if create == True:
            raise NotImplementedError('Creation of a new postgres table is not implemented')
        
        s = self.to_stringIO(transpose, columns)        
        
        if overwrite == True:
            sql = 'DELETE FROM %(schema)s.%(table)s;' % sql_dict
            cursor.execute(sql)
        
        sql = '%(schema)s.%(table)s' % sql_dict
        cursor.copy_from(s, sql, sep = ',', null = '')
        connection.commit()    
        
        # release the string io object
        s.close()      


if __name__ == '__main__':
    
    xls_file = '/Users/mgleason/NREL_Projects/github/diffusion/excel/scenario_inputs.xlsm'
    wb = xl.load_workbook(xls_file, data_only = True)
    fnr = FancyNamedRange(wb, 'derate_factors_wind')    
#    print fnr.data_frame
    
    
