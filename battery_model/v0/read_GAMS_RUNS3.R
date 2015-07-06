
# --- Libraries: ---
library(reshape2)
library(ggplot2)


# ---  Code parameters: ---
read_runs = "t";  # read model runs
calc_optimal = "t"; # find optimal sizing based on dispatch
run_plots = "f";  # plot results
push_to_xlsx = "t"; 

read_salesInst = "t";
read_salesAvg = "f";
read_salesZero = "f";




# --- sAMPLE Model Inputs: -------------------------------------
B_hr_storage = 6;     # number of hours of storage
B_min_OP = 0;         # min storage to be in 'operating' mode
B_max_OP = vB[i2];      # storage capacity (kW)
B_rTrip_eff = .8;      # round trip efficiency of the battery (<=1)
operating_cost = 0;   # variable operating cost ($/kWh)
PVscale = vPV[i1]; #1000;       # size of PV system (kW)
eSoldCap = 1000;      # maximum amount of energy that can be net metered in a given hour (kW)
DReff = 1;            # round trip efficiency of DR (<=1)
DRmin_OP = 0;         # minimum operating mode (0 to 1)
DRmax_OP = 1;         # maximum operating mode (0 to 1)
DRmax    = vDR[i3];       # maximum power output, comparable to generation (kW)
DRmin    = -1*vDR[i3];      # maximum power input, comparable to storage (kW)
DRmax_ST_hr = 4;       # number of hours of storage in a 'charged' state, e.g. thermal storage like chilled water
DRmax_DP_hr = -1;     # number of hours of depleted energy in a 'discharged' state; e.g. warmer temps in summer


#-----  RUN MODEL: ----------------------------------------------
if (read_runs == "t"){
  
vPV = seq(from=0, to=5000, by=200); # PV size in kW
vB  = seq(from=0, to=4000, by=200); # battery size in kW
vB2 = seq(from=1, to=8, by=1); # total hours of DR storage


Tcost = array(0, c(length(vPV), length(vB), length(vB2))); # total cost
Ebought = array(0, c(length(vPV), length(vB), length(vB2))); # total cost
Ecost = array(0, c(length(vPV), length(vB), length(vB2))); # energy costs
Dcost = array(0, c(length(vPV), length(vB), length(vB2))); # demand based costs
SalesRev = array(0, c(length(vPV), length(vB), length(vB2))); # total cost
PVenergy = array(0, c(length(vPV), length(vB), length(vB2))); # PV energy used
PVsold = array(0, c(length(vPV), length(vB), length(vB2))); # PV energy sold instantaneously

t0=proc.time();

for (i1  in 1:length(vPV)){
for (i2  in 1:length(vB)){
for (i3  in 1:length(vB2)){

folder1 = "C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/RUNS/";

# -- set folder: --
if (read_salesInst == "t"){ folder2=paste(folder1, "RUNS_B80eff/", sep="") };
if (read_salesAvg  == "t"){ folder2=paste(folder1, "RUNS_B80eff_EsoldAvg/", sep="") };
if (read_salesZero == "t"){ folder2=paste(folder1, "RUNS_B80eff_EsoldZero/", sep="") };
  
# -- set filename: --  
filenm = paste(folder2, "PV_B_DR_pv", as.character(vPV[i1]), 
                        "_Bc", as.character( vB[i2]), 
                        "_Bhr", as.character( vB2[i3]),
                        "_DRc0_sum.csv", sep="");
# -- read data: --
rdata = read.csv(filenm, header = FALSE) 

Tcost[i1,i2,i3] = rdata[28,2];
Ecost[i1,i2,i3] = rdata[21,2];
Dcost[i1,i2,i3] = rdata[25,2];
#Ebought[i1,i2,i3] = rdata[,2];
SalesRev[i1,i2,i3] = rdata[27,2]
PVenergy[i1,i2,i3] = rdata[7,2];
PVsold[i1,i2,i3] = rdata[9,2];

}; # ---- end  B hours loop: -------
}; # ---- end  B size loop: --------
}; # ---- end PV size loop: --------

Rtime = as.numeric(proc.time() - t0)[3]; # get total computational time in seconds  
print(paste(" . . read time: ", as.numeric(Rtime), " s . . ", sep=""))

}; # ---------- END RUN MODEL: ---------------
#-------------------------------------------------------------------------------------------









#-----------  CALCULATE THE OPTIMAL SIZING: -------------------------------

if (calc_optimal == "t"){  
  
#------ Find Optimal Mix of Battery and PV: ---------
costPV=seq(from=250, to=2500, by=250); # PV cost in $/kW - after any incentives
costB=seq(from=50, to=1000, by=50); # Battery cost in $/kWh - after any incentives 

ann_CRF = 0.1; # Annual capital recovery factor

#-- battery cost array: -- 
E_cost = array(0, c(length(costPV), length(costB) ) ); # electricity cost
E0_cost = array(0, c(length(costPV), length(costB) ) ); # original electricity cost
E_use = array(0, c(length(costPV), length(costB) ) ); # original electricity use
Energy_cost = array(0, c(length(costPV), length(costB) ) ); # original electricity cost
Demand_cost = array(0, c(length(costPV), length(costB) ) ); # original electricity cost
Sales_rev = array(0, c(length(costPV), length(costB) ) ); # original electricity cost
Total_cost = array(1e9, c(length(costPV), length(costB) ) ); # total cost
PVcap = array(0, c(length(costPV), length(costB) ) ); # total cost
Bcap = array(0, c(length(costPV), length(costB) ) ); # total cost
Bhr = array(0, c(length(costPV), length(costB) ) ); # total cost
CapCost = array(0, c(length(costPV), length(costB) ) ); # total cost


# --- PV only, no Battery: -------------------------------------------------------
pv_E_cost = array(0, c(length(costPV) ) ); # electricity cost
pv_E0_cost = array(0, c(length(costPV) ) ); # original electricity cost
pv_E_use = array(0, c(length(costPV) ) ); # original electricity use
pv_Energy_cost = array(0, c(length(costPV) ) ); # original electricity cost
pv_Demand_cost = array(0, c(length(costPV) ) ); # original electricity cost
pv_Sales_rev = array(0, c(length(costPV) ) ); # original electricity cost
pv_Total_cost = array(1e9, c(length(costPV) ) ); # total cost
pv_PVcap = array(0, c(length(costPV) ) ); # total cost
pv_Bcap = array(0, c(length(costPV) ) ); # total cost
pv_Bhr = array(0, c(length(costPV) ) ); # total cost
pv_CapCost = array(0, c(length(costPV) ) ); # total cost

# --- Battery only, no PV: -------------------------------------------------------
b_E_cost = array(0, length(costB) ); # electricity cost
b_E0_cost = array(0, length(costB) ); # original electricity cost
b_E_use = array(0, length(costB) ); # original electricity use
b_Energy_cost = array(0, length(costB) ); # original electricity cost
b_Demand_cost = array(0, length(costB) ); # original electricity cost
b_Sales_rev = array(0,  length(costB) ); # original electricity cost
b_Total_cost = array(1e9, length(costB) ); # total cost
b_PVcap = array(0, length(costB) ); # total cost
b_Bcap = array(0, length(costB) ); # total cost
b_Bhr = array(0,  length(costB) ); # total cost
b_CapCost = array(0, length(costB) ); # total cost

#-- run cost: --
for (i1  in 1:length(vPV)){
for (i2  in 1:length(vB)){
for (i3  in 1:length(vB2)){        
for (i4 in 1:length(costPV)){
for (i5  in 1:length(costB)){
dCost = Tcost[i1,i2,i3] + 
        vPV[i1]*costPV[i4]*ann_CRF + 
        vB[i2]*vB2[i3]*costB[i5]*ann_CRF; # capital cost * annual CRF

# --- All variables: ------
if (dCost < Total_cost[i4,i5]){
  Total_cost[i4,i5] = dCost;
  E_cost[i4,i5] = Tcost[i1,i2,i3];
  E0_cost[i4,i5] = Tcost[1,1,1];
  Energy_cost[i4,i5] = Ecost[i1,i2,i3];
  Demand_cost[i4,i5] = Dcost[i1,i2,i3];
  Sales_rev[i4,i5] = SalesRev[i1,i2,i3];
  PVcap[i4,i5] = vPV[i1];
  Bcap[i4,i5] = vB[i2];
  Bhr[i4,i5] = vB2[i3];
  if (vB[i2] == 0){Bhr[i4,i5] = vB2[i3]*0}; # set hrs = 0 if capacity = 0;
  CapCost[i4,i5] = vPV[i1]*costPV[i4]*ann_CRF + vB[i2]*vB2[i3]*costB[i5]*ann_CRF; # capital cost * annual CRF
}; # end if loop



# --- PV only, no battery: ------
if (dCost < pv_Total_cost[i4] && i2 == 1 && i3 == 1){
  pv_Total_cost[i4] = dCost;
  pv_E_cost[i4] = Tcost[i1,i2,i3];
  pv_E0_cost[i4] = Tcost[1,1,1];
  pv_Energy_cost[i4] = Ecost[i1,i2,i3];
  pv_Demand_cost[i4] = Dcost[i1,i2,i3];
  pv_Sales_rev[i4] = SalesRev[i1,i2,i3];
  pv_PVcap[i4] = vPV[i1];
  pv_Bcap[i4] = vB[i2];
  pv_Bhr[i4] = vB2[i3];
  if (vB[i2] == 0){pv_Bhr[i4] = vB2[i3]*0}; # set hrs = 0 if capacity = 0;
  pv_CapCost[i4] = vPV[i1]*costPV[i4]*ann_CRF; # capital cost * annual CRF
  }; # end if loop



# --- Battery only, no PV: ---
if (dCost < b_Total_cost[i5] && i1 == 1){
  b_Total_cost[i5] = dCost;
  b_E_cost[i5] = Tcost[i1,i2,i3];
  b_E0_cost[i5] = Tcost[1,1,1];
  b_Energy_cost[i5] = Ecost[i1,i2,i3];
  b_Demand_cost[i5] = Dcost[i1,i2,i3];
  b_Sales_rev[i5] = SalesRev[i1,i2,i3];
  b_PVcap[i5] = vPV[i1];
  b_Bcap[i5] = vB[i2];
  b_Bhr[i5] = vB2[i3];
  if (vB[i2] == 0){b_Bhr[i5] = vB2[i3]*0}; # set hrs = 0 if capacity = 0;
  b_CapCost[i5] = vB[i2]*vB2[i3]*costB[i5]*ann_CRF; # capital cost * annual CRF
}; # end if loop

}}}}}; # end for loops 

Cost_svg = (E0_cost-Total_cost)/E0_cost * 100;
b_Cost_svg = (b_E0_cost-b_Total_cost)/b_E0_cost * 100;
pv_Cost_svg = (pv_E0_cost-pv_Total_cost)/pv_E0_cost * 100;  
  
}; # --- end CALC OPTIMAL --------------------------------------------





#--------- write DATA TO EXCEL: ----------------------------------------------------------
if (push_to_xlsx == "t"){

require(XLConnect)

# set file path
Dfolder="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/DATA_SUMMARY/"; 
Dfile=paste(Dfolder, "Optimal_PV_Battery_OUTPUT.xlsx", sep="") ;

if (read_salesInst == "t"){ sht="eSales_hourlyPrice" };
if (read_salesAvg  == "t"){ sht="eSales_annualAvgPrice"  };
if (read_salesZero == "t"){ sht="eSales_zeroPrice"  };

# create workbook
wb = loadWorkbook(Dfile, create = TRUE);

# Create a worksheet called 'Cost'
createSheet(wb, name = sht)

# -- write Cost savings: ---
d=data.frame(Cost_svg);
d2=t(as.numeric(b_Cost_svg));
d3=as.numeric(pv_Cost_svg);
d4=0;
names(d)=as.character(costB);
writeWorksheet(wb, d, sheet = sht, startRow = 7, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 18, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 8, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 18, startCol = 24, header=FALSE)


# -- write PV Capacity: ---
d=data.frame(PVcap);
names(d)=as.character(costB);
d2=t(as.numeric(b_PVcap));
d3=as.numeric(pv_PVcap);
d4=0;
writeWorksheet(wb, d, sheet = sht, startRow = 24, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 35, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 25, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 35, startCol = 24, header=FALSE)

# -- write Battery Capacity: ---
d=data.frame(Bcap);
names(d)=as.character(costB);
d2=t(as.numeric(b_Bcap));
d3=as.numeric(pv_Bcap);
d4=0;
writeWorksheet(wb, d, sheet = sht, startRow = 41, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 52, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 42, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 52, startCol = 24, header=FALSE)


# -- write Battery hours: ---
d=data.frame(Bhr);
names(d)=as.character(costB);
d2=t(as.numeric(b_Bhr));
d3=as.numeric(pv_Bhr);
d4=0;
writeWorksheet(wb, d, sheet = sht, startRow = 58, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 69, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 59, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 69, startCol = 24, header=FALSE)



# -- write Electricity cost ($/yr): ---
d=data.frame(E_cost);
names(d)=as.character(costB);
d2=t(as.numeric(b_E_cost));
d3=as.numeric(pv_E_cost);
d4=E0_cost[1,1];
writeWorksheet(wb, d, sheet = sht, startRow = 75, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 86, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 76, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 86, startCol = 24, header=FALSE)


# -- write Energy cost ($/yr): ---
d=data.frame(Energy_cost);
names(d)=as.character(costB);
d2=t(as.numeric(b_Energy_cost));
d3=as.numeric(pv_Energy_cost);
d4=Ecost[1,1,1];
writeWorksheet(wb, d, sheet = sht, startRow = 92, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 103, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 93, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 103, startCol = 24, header=FALSE)


# -- write Demand cost ($/yr): ---
d=data.frame(Demand_cost);
names(d)=as.character(costB);
d2=t(as.numeric(b_Demand_cost));
d3=as.numeric(pv_Demand_cost);
d4=Dcost[1,1,1];
writeWorksheet(wb, d, sheet = sht, startRow = 109, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 120, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 110, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 120, startCol = 24, header=FALSE)

# -- Sales Revenue ($/yr): ---
d=data.frame(Sales_rev);
names(d)=as.character(costB);
d2=t(as.numeric(b_Sales_rev));
d3=as.numeric(pv_Sales_rev);
d4=0;
writeWorksheet(wb, d, sheet = sht, startRow = 126, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 137, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 127, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 137, startCol = 24, header=FALSE)


# -- PV & Battery Capital Cost ($/yr): ---
d=data.frame(CapCost);
names(d)=as.character(costB);
d2=t(as.numeric(b_CapCost));
d3=as.numeric(pv_CapCost);
d4=0;
writeWorksheet(wb, d, sheet = sht, startRow = 143, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 154, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 144, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 154, startCol = 24, header=FALSE)



# -- Total Electricity Cost ($/yr): ---
d=data.frame(Total_cost);
names(d)=as.character(costB);
d2=t(as.numeric(b_Total_cost));
d3=as.numeric(pv_Total_cost);
d4=E0_cost[1,1];
writeWorksheet(wb, d, sheet = sht, startRow = 160, startCol = 4)
writeWorksheet(wb, d2, sheet = sht, startRow = 171, startCol = 4, header=FALSE)
writeWorksheet(wb, d3, sheet = sht, startRow = 161, startCol = 24, header=FALSE)
writeWorksheet(wb, d4, sheet = sht, startRow = 171, startCol = 24, header=FALSE)




# -- General Numbers
totE = 7646296; # sum(df_hrly$eLoad);  total electricity use by building before storage losses;
writeWorksheet(wb, Tcost[1,1,1], sheet = sht, startRow = 177, startCol = 4, header=FALSE); # total electricity cost 
writeWorksheet(wb, Ecost[1,1,1], sheet = sht, startRow = 178, startCol = 4, header=FALSE); # total energy cost 
writeWorksheet(wb, Dcost[1,1,1], sheet = sht, startRow = 179, startCol = 4, header=FALSE); # total demand cost
writeWorksheet(wb, totE,         sheet = sht, startRow = 180, startCol = 4, header=FALSE); # total electricity use
writeWorksheet(wb, Tcost[1,1,1]/totE, sheet = sht, startRow = 181, startCol = 4, header=FALSE); # total electricity use
saveWorkbook(wb)

  
};
#--------- end DATA TO EXCEL: ----------------------------------------------------------










# --- PLOTTING OUTPUT: -------------------------------------------------------------------

if (run_plots == "t"){

Pfolder="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/PLOTS/"  
  
  library(grid)
  library(extrafont)
  library(directlabels)
  library(R.utils) # to source a full path; update only files that have changed             #
  # add all functions from this directory                                                   #
  sourceDirectory("C:/Users/edrury/Dropbox/R_LIBRARY/", modifiedOnly=TRUE);   #
  
  
# -- plot ELECTRICITY SAVINGS: ------------------------------------------------
 ppi <- 200
 png(paste(Pfolder,"CostSavings.png",sep=""), width=7*ppi, height=6*ppi, res=ppi)

  v=Cost_svg;
  a2=melt(v);
  names(a2)=c("PV", "Battery", "z")
  v = ggplot(a2, aes(x=Battery, y=PV, z = z))
  v = v + stat_contour(aes(colour = ..level..), size = 1.5) 
  v = v + geom_dl(aes(label=..level.., colour=..level..),
                   data=a2, method="bottom.pieces", stat="contour")
  v = v + labs(x= "Battery Cost ($/kWh)",
               y="PV Cost after incentives ($/kW)", 
               title="Savings on Annual Electricity Costs (%)")
v = v + scale_x_discrete(labels=c(as.character(costB)), limits=c(as.character(costB)))
v = v + scale_y_discrete(labels=c(as.character(costPV)), limits=c(as.character(costPV)))
  v + theme(axis.title = element_text(face="bold", colour="#000099", size=16),
            axis.text.x  = element_text(face="bold", colour="#333333", angle=55, vjust=0.5, size=12),
            axis.text.y  = element_text(face="bold", colour="#333333", angle=15, vjust=0.5, size=12),
            plot.title = element_text(face="bold", colour="#000099", hjust=0, size=18),
            panel.background = element_rect(fill = '#F8F8F8'),
            panel.grid.major = element_line(colour = '#E0E0E0'),
            legend.position="none")
  dev.off()
#-------------- end ELECTRICITY SAVINGS: ------------------------------------------------------  





# -- plot PV SIZE: ------------------------------------------------
ppi <- 200
png(paste(Pfolder,"OptimalPVCapacity.png",sep=""), width=7*ppi, height=6*ppi, res=ppi)

v=PVcap;
a2=melt(v);
names(a2)=c("PV", "Battery", "z")
v = ggplot(a2, aes(x=Battery, y=PV, z = z))
v = v + stat_contour(aes(colour = ..level..), size = 1.5) 
v = v + geom_dl(aes(label=..level.., colour=..level..),
                data=a2, method="bottom.pieces", stat="contour")
v = v + labs(x= "Battery Cost ($/kWh)",
             y="PV Cost after incentives ($/kW)", 
             title="Optimal PV Capacity (kW)")
v = v + scale_x_discrete(labels=c(as.character(costB)), limits=c(as.character(costB)))
v = v + scale_y_discrete(labels=c(as.character(costPV)), limits=c(as.character(costPV)))
v + theme(axis.title = element_text(face="bold", colour="#000099", size=16),
          axis.text.x  = element_text(face="bold", colour="#333333", angle=55, vjust=0.5, size=12),
          axis.text.y  = element_text(face="bold", colour="#333333", angle=15, vjust=0.5, size=12),
          plot.title = element_text(face="bold", colour="#000099", hjust=0, size=18),
          panel.background = element_rect(fill = '#F8F8F8'),
          panel.grid.major = element_line(colour = '#E0E0E0'),
          legend.position="none")
dev.off()
#-------------- end PV SIZE: ------------------------------------------------------  




# -- plot Battery SIZE: ------------------------------------------------
ppi <- 200
png(paste(Pfolder,"OptimalBatteryCapacity.png",sep=""), width=7*ppi, height=6*ppi, res=ppi)

v=Bcap;
a2=melt(v);
names(a2)=c("PV", "Battery", "z")
v = ggplot(a2, aes(x=Battery, y=PV, z = z))
v = v + stat_contour(aes(colour = ..level..), size = 1.5) 
v = v + geom_dl(aes(label=..level.., colour=..level..),
                data=a2, method="bottom.pieces", stat="contour")
v = v + labs(x= "Battery Cost ($/kWh)",
             y="PV Cost after incentives ($/kW)", 
             title="Optimal Battery Capacity (kW)")
v = v + scale_x_discrete(labels=c(as.character(costB)), limits=c(as.character(costB)))
v = v + scale_y_discrete(labels=c(as.character(costPV)), limits=c(as.character(costPV)))
v + theme(axis.title = element_text(face="bold", colour="#000099", size=16),
          axis.text.x  = element_text(face="bold", colour="#333333", angle=55, vjust=0.5, size=12),
          axis.text.y  = element_text(face="bold", colour="#333333", angle=15, vjust=0.5, size=12),
          plot.title = element_text(face="bold", colour="#000099", hjust=0, size=18),
          panel.background = element_rect(fill = '#F8F8F8'),
          panel.grid.major = element_line(colour = '#E0E0E0'),
          legend.position="none")
dev.off()
#-------------- end Battery SIZE: ------------------------------------------------------ 



# -- plot Battery Storage Hours: ------------------------------------------------
ppi <- 200
png(paste(Pfolder,"OptimalBatteryStorageHours.png",sep=""), width=7*ppi, height=6*ppi, res=ppi)

v=Bhr;
a2=melt(v);
names(a2)=c("PV", "Battery", "z")
v = ggplot(a2, aes(x=Battery, y=PV, z = z))
v = v + stat_contour(aes(colour = ..level..), size = 1.5) 
v = v + geom_dl(aes(label=..level.., colour=..level..),
                data=a2, method="bottom.pieces", stat="contour")
v = v + labs(x= "Battery Cost ($/kWh)",
             y="PV Cost after incentives ($/kW)", 
             title="Optimal Battery Storage Hours (hours)")
v = v + scale_x_discrete(labels=c(as.character(costB)), limits=c(as.character(costB)))
v = v + scale_y_discrete(labels=c(as.character(costPV)), limits=c(as.character(costPV)))
v + theme(axis.title = element_text(face="bold", colour="#000099", size=16),
          axis.text.x  = element_text(face="bold", colour="#333333", angle=55, vjust=0.5, size=12),
          axis.text.y  = element_text(face="bold", colour="#333333", angle=15, vjust=0.5, size=12),
          plot.title = element_text(face="bold", colour="#000099", hjust=0, size=18),
          panel.background = element_rect(fill = '#F8F8F8'),
          panel.grid.major = element_line(colour = '#E0E0E0'),
          legend.position="none")
dev.off()
#-------------- end Battery SIZE: ------------------------------------------------------ 









}; #--- end plot
#----------- END PLOT LOOP: -----------------------------------------------




