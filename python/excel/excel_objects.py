# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 12:09:03 2015

@author: mgleason
"""

import openpyxl as xl
from cStringIO import StringIO
import numpy as np
import pandas as pd
import excel_functions as xl_funcs


class ExcelError(Exception):
    pass


class FancyNamedRange(object):
    
    def __init__(self, openpyxl_named_range):
        self.base = openpyxl_named_range
        
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
        
        coordinates = self.cell_ranges[0].split(':')[0]
        
        return coordinates
        
    def __bottomright__(self):
        
        coordinates = self.cell_ranges[0].split(':')[1]
        
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
        
        return cell_array
        
    def __cell_value__(self, cell):
        
        return cell.value
    
    def __rec_array__(self):
        
        cell_values = np.vectorize(self.__cell_value__)
        
        cols = []
        for j in range(self.cell_array.shape[1]):
            col = cell_values(self.cell_array[:, j])
            cols.append(col)
        
        rec_array = np.rec.fromarrays(cols)
        
        return rec_array
    
    def __data_frame__(self):
        
        df = pd.DataFrame(self.rec_array)
        ncols = df.shape[1]
        df.columns = range(0, ncols)
        
        return df
        
    
    def to_stringIO(self, columns = None, index = False, header = False):
        
        s = StringIO()
        
        if columns == None:
            columns = self.data_frame.columns
            
        self.data_frame[columns].to_csv(s, index, header)
        
        s.seek(0)
        
        return s
        
    def to_postgres(self, connection, cursor, schema, table, columns = None, create = False, overwrite = True):
        
        sql_dict = {'schema': schema, 'table': table}
        
        if create == True:
            raise NotImplementedError('Creation of a new postgres table is not implemented')
        
        s = self.to_stringIO(columns)        
        
        if overwrite == True:
            sql = 'DELETE FROM %(schema)s.%(table)s;' % sql_dict
            cursor.execute(sql)
        
        sql = '%(schema)s.%(table)s;' % sql_dict
        cursor.copy_from(s, sql, sep=',')
        connection.commit()    
        
        # release the string io object
        s.close()      


if __name__ == '__main__':
    
    xls_file = '/Users/mgleason/NREL_Projects/github/diffusion/excel/scenario_inputs.xlsm'
    wb = xl.load_workbook(xls_file, data_only = True)
    nr = xl_funcs.get_named_range(wb, 'parcel_size_wind')
    fnr = FancyNamedRange(nr)    
    fnr.data_frames
    
    
