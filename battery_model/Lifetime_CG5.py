# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 15:35:33 2015

@author: cdong
"""

import numpy;

# Battery functions
# from battery import *;

# return codes possible
LT_SUCCESS = 0;
LT_GET_DATA = 1;
LT_RERANGE = 2;

# Lifetime calculations
def life_vs_DOD(dod, t, t0, a1, a2, a3, a4, a5):
    return (a1*(1. + (dod-0.2)*a2)*t**(1./2.) + a3*(1. + (dod-0.2)*a2)*(t - (t0-(dod/0.2-1.)*a4))
    *(t>(t0 - (dod/0.2-1.)*a4)) + a5);

# start here
class Lifetime:

    # class Variables  
    Xlt=0;
    Ylt=0;
    Slt=0;
    jlt=0; # last index in Peaks, i.e. Peaks = [0.1, 0.2, 0.23], jlt = 2;
    klt=0; # current index in Peaks where Slt is stored
    Dlt=0;
    Peaks=[];
    Range=0;
    DOD = 0;
    vDOD = [];
    cumDOD = 0;
    #  vcumDOD = [];
    avgDOD = 0;
    retCode=0;
    t0 = 0;
    a1= 0;
    a2=0;
    a3= 0;
    a4= 0;
    a5 = 0;   # starting capacity in capacity fade curve
    # qcap = 16000; 
    # qmax = qcap;
    nCycles = 0;
    eqnCycles = 0;
    S_rereadCount = 0;

    
    def __init__(self, a_vect, Dlt):
        self.t0 = a_vect[0];
        self.a1 = a_vect[1];
        self.a2 = a_vect[2];
        self.a3 = a_vect[3];
        self.a4 = a_vect[4];
        self.a5 = a_vect[5];
        self.Dlt = Dlt;
    
    def rainflow(self, DOD, Dlt):
        
        # initialize return code;
        retCode = LT_GET_DATA;
        
        # Begin algorithm 
        self.Peaks.append(DOD);
        atStepTwo = True;
        
        # Assign S, which is the starting peak or valley
        if self.jlt == 0:
            self.Slt = DOD;
            self.klt = self.jlt;
            
        # Loop forever until break
        while atStepTwo:
            
            # LT: Step 2: Form ranges X,Y
            if self.jlt >= 2:
                if self.jlt - 2 >= self.klt:
                    self.rainflow_ranges(self.Peaks, self.jlt);

            else:
                # Get more data (Step 1)
                retCode = LT_GET_DATA;
                break;
            
            # LT: Step 3: Compare ranges
#           print "r1:  %s " % retCode;
            retCode = self.rainflow_compareRanges(self.Peaks, self.Range, self.Slt, self.Xlt, self.Ylt, self.jlt, self.klt);     
           # print contained;
           
            # We break to get more data, or if we are done with step 5
            if retCode == LT_GET_DATA:
                break;
        
        # Step 6?
        if retCode == LT_GET_DATA:
            self.jlt += 1;
        
        # update return code;
        self.retCode = retCode;
        
        # return updated damage
        return self.nCycles, self.Dlt;
    
    
    def rainflow_ranges(self, Peaks, jlt):
        # Step 2: Compute ranges
        self.Ylt = abs(Peaks[jlt-1] - Peaks[jlt-2]);
        self.Xlt = abs(Peaks[jlt] - Peaks[jlt-1] );
       # print self.Ylt, self.Xlt;

    def rainflow_ranges_circular(self, Peaks, jlt):
        
        if jlt == 0:
            self.Xlt = abs(Peaks[0] - Peaks[-1]);
            self.Ylt = abs(Peaks[-1] - Peaks[-2]);
          #  print self.Ylt, self.Xlt;
        elif jlt == 1:
            self.Xlt = abs(Peaks[1] - Peaks[0]);
            self.Ylt = abs(Peaks[0] - Peaks[-1]);
          #  print self.Ylt, self.Xlt;
        else :
            self.rainflow_ranges(Peaks,jlt);
            
    
    def rainflow_compareRanges(self, Peaks, Range, Slt, Xlt, Ylt, jlt, klt):   
        
        retCode = LT_SUCCESS;
        contained = True;
        
      #  print self.Peaks;                    
        # Step 3 Compare ranges
        if Xlt < Ylt:
            retCode = LT_GET_DATA;
        elif (Xlt == Ylt): 
            if (Slt == Peaks[jlt-1]) or (Slt == Peaks[jlt-2]):
                retCode = LT_GET_DATA;
            else:
                contained = False;
        elif (Xlt >= Ylt):
            if (Xlt > Ylt):
                
                if self.klt == jlt - 1:
                    # Step 4: Move S to next point in Vector, then go to Step 1
                    #  print Slt;
                 #   print("starting point index: %f" %(self.Slt));
                    retCode = LT_GET_DATA;
                elif self.klt == jlt - 2 :
                    self.klt+=1;
                   # print("starting point has moved");
                    self.Slt = Peaks[self.klt];
                  #  print self.klt;
                  #  print("starting point index: %f" %(self.Slt));
                    retCode = LT_GET_DATA;
                else:
                 #   print("starting point index: %f" %(self.Slt));
                    contained = False;
                  #  print contained;
            elif (Xlt == Ylt):
                if (Slt != Peaks[jlt-1]) and (Slt != Peaks[jlt-2]):
                    contained = False;
        
        # Step 5: Count range Y, discard peak & valley of Y, go to Step 2
        if not contained:
            self.Range = Ylt;
            self.DOD = self.Range/self.qmax;
            self.vDOD.append(self.DOD);
            print self.DOD;
            self.cumDOD += self.DOD;
           # print("starting point index: %f" %(self.Slt));     
            self.nCycles += 1;
            self.eqnCycles = self.cumDOD / self.DOD;
            self.avgDOD = self.cumDOD / self.nCycles;
            Cf = life_vs_DOD(self.DOD, self.eqnCycles, self.t0, self.a1,self.a2,self.a3,self.a4,self.a5);
            Cf_last = life_vs_DOD(self.DOD, self.eqnCycles-1, self.t0, self.a1,self.a2,self.a3,self.a4,self.a5)
           # print Cf;
           # print self.Dlt;
           # self.Dlt += 1./self.eqnCycles * (1 - Cf) * self.qmax;
            self.Dlt += (Cf_last - Cf)*self.qmax;
            print self.Dlt;
            self.qmax = self.qcap - self.Dlt;
            print self.qmax;
            # Discard peak and valley of Y
            del self.Peaks[-2];
            del self.Peaks[-2];
           # print self.Peaks;
            self.jlt-=2;
            # stay in the while loop
            retCode = LT_RERANGE;
           # retCode = LT_GET_DATA;            
     
        return retCode;       
    
    def finish(self):
                
        print
        print "The Finish Function Starts Here"
        print
       # Starting index.
        ii = 0;
        self.jlt -= 1;
       # print self.jlt;
        
        # Start in a place with data before and after
        while self.S_rereadCount <= 1 :
            
            if ii < len(self.Peaks):
               # P = self.Peaks[ii];
                P = self.Peaks[ii];
              #  print P;
              #  print self.Peaks;
                self.Peaks.append(P);
                #self.Peaks.append(self.Peaks[0:ii+1]);
             #   print self.Peaks;
            else:
                break;
            
            # Step 6
            # if (P == self.Slt):
            if (self.klt <= ii):
             #   print("Number of reRead: %d" %(self.S_rereadCount));
            #if self.Slt not in P: 
                self.S_rereadCount += 1;
             #   print self.S_rereadCount;
             #   print("Number of reRead: %d" %(self.S_rereadCount));
                
            atStepSeven = True;
            
            mlt = 0;
            # LT: Step 7: Form ranges X,Y
            while atStepSeven:
                mlt = self.jlt + ii + 1;
                if mlt >= 2:            
                    
                    self.rainflow_ranges(self.Peaks, mlt);
                    
                else:
                    atStepSeven = False;
                    break;
        
                # LT: Step 8: Compare X and Y
                if self.Xlt < self.Ylt:
                    atStepSeven = False;
                    # move to next point (Step 6)
                  #  print ii;
                    ii += 1;
                  #  print ii;
                else:
                    self.Range=(self.Ylt);
                   # print 
                   # print self.klt; 
                   # print mlt;
                    self.DOD = self.Range/self.qmax;
                    self.vDOD.append(self.DOD);
                    self.cumDOD += self.DOD;
                    self.nCycles += 1;
                    self.eqnCycles = self.cumDOD / self.DOD;                    
                    self.avgDOD = self.cumDOD / self.nCycles;
                    # print("starting point index: %f" %(self.Slt));     

                   # Cf = life_vs_DOD(self.DOD, self.eqnCycles, self.t0, self.a1,self.a2,self.a3,self.a4,self.a5);
                   # self.Dlt += 1./self.eqnCycles * (1 - Cf) * self.qmax;
                    Cf = life_vs_DOD(self.DOD, self.eqnCycles, self.t0, self.a1,self.a2,self.a3,self.a4,self.a5);
                    Cf_last = life_vs_DOD(self.DOD, self.eqnCycles-1, self.t0, self.a1,self.a2,self.a3,self.a4,self.a5)
                    self.Dlt += (Cf_last - Cf)*self.qmax;
                    self.qmax = self.qcap - self.Dlt;                
                    # Discard peak and valley of Y, which is ii-1, ii-2
                   # print
                   # print self.Peaks;
                   # print("Number of Cycles: %d" %(self.nCycles));
                    
                    if (ii >= self.klt):
#                    if (self.klt == mlt - 1) or (self.klt == mlt - 2):
#                        self.klt -= 1;  # go backwards, which is not okay.
                     #   print self.klt;
                        self.S_rereadCount += 1;
                    #    print self.S_rereadCount;

                    del self.Peaks[-2];
                    del self.Peaks[-2];
                   # print self.Peaks;
                    self.jlt-=2;
                    
                    if self.S_rereadCount == 2:
                        break;
            
        return self.nCycles, self.Dlt, self.vDOD, self.qmax;
       # print self.Dlt, self.qmax;
