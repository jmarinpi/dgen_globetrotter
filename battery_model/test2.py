"""
Created on Tue Jun  9 09:01:10 2015
@author: cdong
"""


import sys;
import numpy as np;
import pandas as pd;
from Lifetime_CG5 import Lifetime, life_vs_DOD;
#from scipy import interpolate;
#import matplotlib.pyplot as plt;

def main(qcap, qmax):
    
    SOC = pd.read_csv('C:\GamsProject_CG\Replicate\Lifetime\SOC.csv')
    SOC = np.array(SOC['x'].tolist())
    
    a_vect = np.array([1825, -2.56e-3, 1.1333, -9.12e-5, 300, 1]);
    
    Dlt = 0;
    Peaks = [];
    nCycles = 0;
    
    # supply qcap from system call
    Lifetime.qcap = float(qcap);
    Lifetime.qmax = float(qmax);
    
    lt = Lifetime(a_vect, Dlt); 
    
    for i, d in enumerate(SOC):
        print 
        print i
        nCycles = lt.rainflow(d, Dlt);
    
        print("Number of Cycles: %d" %(lt.nCycles));
    
    # call the finish function
    nCycles, Dlt, vDOD, qmax  = lt.finish();
    
    print("Number of Cycles: %d" % (lt.nCycles));
    print("Cumulative Loss in kWh: %0.0f" % (lt.Dlt));
    print("Remaining Capacity in kWh: %0.0f" % (lt.qmax));
    print("Average DOD: %0.3f" % (lt.avgDOD));
    print("Cumulative DOD for Year One: %0.3f" % (lt.cumDOD));
    
    ## invert life_vs_DOD to find equivalent 'd' in year one
    Cf0 = lt.Dlt / lt.qcap;
    print Cf0;
    
    def f(x):
        return 1 - life_vs_DOD(x, lt.nCycles, lt.t0, lt.a1,lt.a2,lt.a3,lt.a4,lt.a5) - Cf0
    
    def bisection(a, b, tol):
        c = (a + b)/2.
        while (b - a)/2. > tol:
            if f(c) == 0:
                return c
            elif f(a)*f(c) < 0:
                b = c
            else:
                a = c
            c = (a + b) / 2.
        return c
    
    eqnDOD = bisection(0, 1, 0.0001);
    print eqnDOD;
    
    Cf = 0;
    j = 1;
    ratio = 0.6788;
    # qmax = lt.qmax;    
    Vqmax = [lt.qcap];
    Vqmax.append(qmax);
    
    
    while qmax > lt.qcap * ratio:  # 10860.8 as a threshold for ratio as 0.6788
        print
        print("Number of Years: %d" % (j+1));
        Cf = life_vs_DOD(eqnDOD, lt.nCycles*(j+1), lt.t0, lt.a1,lt.a2,lt.a3,lt.a4,lt.a5);
        print Cf, qmax;
        Dlt = (1. - Cf) * lt.qcap;
        qmax = lt.qcap - Dlt;
        print Dlt, qmax;
        Vqmax.append(qmax);
        if qmax < lt.qcap * ratio:
            break;
        j += 1;

    # print Vqmax;
    
    from collections import OrderedDict;
    dict_out = OrderedDict();
    dict_out[columns[0]] = j;
    dict_out[columns[1]] = lt.nCycles;
    dict_out[columns[2]] = Cf0;
    dict_out[columns[3]] = Vqmax;
    print dict_out;
    
    df = pd.DataFrame(dict_out);
    
    df.iloc[1:j+1,0:3] = float('nan')
    
    file_name = 'C:\GamsProject_CG\Replicate\Lifetime\life_Results.csv';
    df.to_csv(file_name, index = False, header=True, na_rep='NA');
    

if __name__ == "__main__":
    main(sys.argv[1:][0], sys.argv[1:][0])
