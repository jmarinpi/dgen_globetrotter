# --- Libraries: ---
library(reshape2)
library(ggplot2)
library(tidyr)  # require package "stringi"

# ---  Code parameters: ---
read_runs = "t";  # read model runs
calc_optimal = "t"; # find optimal sizing based on dispatch
find_opt_size = 't'; # find optimal size based on current PV and Battery price
run_lifetime = 't'; # run lifetime module in Python
run_plots = "f";  # plot results
push_to_xlsx = "f"; 

read_salesInst = "t";
read_salesAvg = "f";
read_salesZero = "f";

PVscale0 = 2000;

# --- SAMPLE Model Inputs: -------------------------------------
B_min_OP = 0;         # min storage to be in 'operating' mode
B_max_OP = 16000;      # storage capacity (kW)
B_rTrip_eff = .8;      # round trip efficiency of the battery (<=1)
operating_cost = 0;   # variable operating cost ($/kWh)
PVscale = PVscale0*0.5;    #1000;  # size of PV system (kW)
eSoldCap = 1000;      # maximum amount of energy that can be net metered in a given hour (kW)
DReff = 1;            # round trip efficiency of DR (<=1)
DRmin_OP = 0;         # minimum operating mode (0 to 1)
DRmax_OP = 1;         # maximum operating mode (0 to 1)
DRmax    = 0;       # maximum power output, comparable to generation (kW)
DRmin    = 0;      # maximum power input, comparable to storage (kW)
DRmax_ST_hr = 6;       # number of hours of storage in a 'charged' state, e.g. thermal storage like chilled water
DRmax_DP_hr = -1;     # number of hours of depleted energy in a 'discharged' state; e.g. warmer temps in summer


#-----  RUN MODEL: ----------------------------------------------
if (read_runs == "t"){

vPV = seq(from=0.5, to=1, by=0.05); # PV size in kW
vB  = seq(from=0, to=16000, by=400); # battery size in kW

Tcost = array(0, c(length(vPV), length(vB))); # total cost
#Ebought = array(0, c(length(vPV), length(vB), length(vB2))); # total cost, old one from Easan
Ecost = array(0, c(length(vPV), length(vB))); # energy costs
Dcost = array(0, c(length(vPV), length(vB))); # demand based costs
SalesRev = array(0, c(length(vPV), length(vB))); # sales revenue (NEM)
PVenergy = array(0, c(length(vPV), length(vB))); # PV energy used (gen - curtailed)
PVsold = array(0, c(length(vPV), length(vB))); # PV energy sold instantaneously 

t0=proc.time();

for (i1  in 1:length(vPV)){
for (i2  in 1:length(vB)){

### Change this
folder1 = "C:/GamsProject_CG/Replicate/RUNS/";

# -- set folder: --
if (read_salesInst == "t"){ folder2=paste(folder1, "RUNS_B80eff_noDR/", sep="") };
if (read_salesAvg  == "t"){ folder2=paste(folder1, "RUNS_B80eff_EsoldAvg/", sep="") };
if (read_salesZero == "t"){ folder2=paste(folder1, "RUNS_B80eff_EsoldZero/", sep="") };

# -- set filename: only need the sum.csv file --  
filenm = paste(folder2, "PV_B_DR_pv", as.character(vPV[i1]), 
                        "_Bc", as.character( vB[i2]), 
                        "_DRc0_sum.csv", sep="");

# ------ read data: from the sum.csv -------
rdata = read.csv(filenm, header = FALSE) 

# very this: if the first index is the correct row number in the sum.csv file
Tcost[i1,i2] = rdata[30,2];
Ecost[i1,i2] = rdata[23,2];
Dcost[i1,i2] = rdata[27,2];
#Ebought[i1,i2,i3] = rdata[,2];
SalesRev[i1,i2] = rdata[29,2]
PVenergy[i1,i2] = rdata[9,2];
PVsold[i1,i2] = rdata[11,2];

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

# --- pv + battery cost array: -- 
E_cost = array(0, c(length(costPV), length(costB) ) ); # electricity cost
E0_cost = array(0, c(length(costPV), length(costB) ) ); # original electricity cost
E_use = array(0, c(length(costPV), length(costB) ) ); # original electricity use
Energy_cost = array(0, c(length(costPV), length(costB) ) ); # original electricity cost
Demand_cost = array(0, c(length(costPV), length(costB) ) ); # original electricity cost
Sales_rev = array(0, c(length(costPV), length(costB) ) ); # original electricity cost
Total_cost = array(1e9, c(length(costPV), length(costB) ) ); # total cost set to be 1e9
PVcap = array(0, c(length(costPV), length(costB) ) ); # null PV capacity
Bcap = array(0, c(length(costPV), length(costB) ) ); # null battery capacity
CapCost = array(0, c(length(costPV), length(costB) ) ); # null capital cost


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
b_CapCost = array(0, length(costB) ); # total cost

#-- run cost: --
for (i1  in 1:length(vPV)){
for (i2  in 1:length(vB)){    
for (i4 in 1:length(costPV)){
for (i5  in 1:length(costB)){
### Remember dCost is only an annual number here
dCost = Tcost[i1,i2] + 
        PVscale0*vPV[i1]*costPV[i4]*ann_CRF + 
        vB[i2]*costB[i5]*ann_CRF;   # capital cost * annual CRF

# --- All variables: ------
if (dCost < Total_cost[i4,i5]){
  Total_cost[i4,i5] = dCost;   # this is essentially a loop to find the minimal dCost for all indeces
  E_cost[i4,i5] = Tcost[i1,i2];
  E0_cost[i4,i5] = Tcost[1,1];  # no PV or Battery
  Energy_cost[i4,i5] = Ecost[i1,i2];
  Demand_cost[i4,i5] = Dcost[i1,i2];
  Sales_rev[i4,i5] = SalesRev[i1,i2];
  PVcap[i4,i5] = vPV[i1];    # optimal PV offset value
  Bcap[i4,i5] = vB[i2];      # optimal battery capacity
#  if (vB[i2] == 0){Bhr[i4,i5] = vB2[i3]*0}; # set hrs = 0 if capacity = 0;
  CapCost[i4,i5] = vPV[i1]*costPV[i4]*ann_CRF + vB[i2]*costB[i5]*ann_CRF; # capital cost * annual CRF
}; # end if loop



# --- PV only, no battery: ------
if (dCost < pv_Total_cost[i4] && i2 == 1){
  pv_Total_cost[i4] = dCost;
  pv_E_cost[i4] = Tcost[i1,i2];
  pv_E0_cost[i4] = Tcost[1,1]; # no PV or Battery
  pv_Energy_cost[i4] = Ecost[i1,i2];
  pv_Demand_cost[i4] = Dcost[i1,i2];
  pv_Sales_rev[i4] = SalesRev[i1,i2];
  pv_PVcap[i4] = vPV[i1];
  pv_Bcap[i4] = vB[i2];
#  if (vB[i2] == 0){pv_Bhr[i4] = vB2[i3]*0}; # set hrs = 0 if capacity = 0;
  pv_CapCost[i4] = PVscale0*vPV[i1]*costPV[i4]*ann_CRF; # capital cost * annual CRF
  }; # end if loop



# --- Battery only, no PV: ---
if (dCost < b_Total_cost[i5] && i1 == 1){
  b_Total_cost[i5] = dCost;
  b_E_cost[i5] = Tcost[i1,i2];
  b_E0_cost[i5] = Tcost[1,1]; # no PV or Battery
  b_Energy_cost[i5] = Ecost[i1,i2];
  b_Demand_cost[i5] = Dcost[i1,i2];
  b_Sales_rev[i5] = SalesRev[i1,i2];
  b_PVcap[i5] = vPV[i1];
  b_Bcap[i5] = vB[i2];
 # if (vB[i2] == 0){b_Bhr[i5] = vB2[i3]*0}; # set hrs = 0 if capacity = 0;
  b_CapCost[i5] = vB[i2]*costB[i5]*ann_CRF; # capital cost * annual CRF
}; # end if loop

}}}}; # end for loops 

### cost saving percentages
Cost_svg = (E0_cost-Total_cost)/E0_cost * 100;
b_Cost_svg = (b_E0_cost-b_Total_cost)/b_E0_cost * 100;
pv_Cost_svg = (pv_E0_cost-pv_Total_cost)/pv_E0_cost * 100;  

}; # --- end CALC OPTIMAL --------------------------------------------


if (find_opt_size == 't'){

# inputs: supply current post-incentive Battery cost ($/kWh) and PV cost ($/kW)
Bcost = 3000/7    # specify it as -9999 for the no Battery case to find the optimal PV size
PVcost = 1800

lookup = data.frame(PVcap);
names(lookup)=paste("BC", as.character(costB), sep="");
row.names(lookup)=as.character(costPV);
lookup$noBat = pv_PVcap;

lookup$costPV <- rownames(lookup) 
lookup$costPV = factor(lookup$costPV)


data_long <- gather(lookup, costB, PVsize, BC50:noBat)

data_long$costBat = as.character(lapply(strsplit(as.character(data_long$costB), split="BC"), "[", 2))
data_long$costB = NULL

data_long$costPV = as.numeric(as.character(data_long$costPV))
data_long$costBat = as.numeric(as.character(data_long$costBat))

data_long = data_long[,c(3,1,2)]

data_long$costBat[is.na(data_long$costBat)] = -9999

data_long$dist = (Bcost - data_long$costBat)^2 + (PVcost - data_long$costPV)^2
optPVsize = data_long[which.min(data_long$dist), "PVsize"]

#----------- Find Optimal Battery Size for Known Cost -------------------

lookup = data.frame(Bcap);
names(lookup)=paste("BC", as.character(costB), sep="");
lookup = rbind(lookup, b_Bcap);
row.names(lookup) = c(as.character(costPV), "noPV");


lookup$costPV <- c(as.character(costPV), "noPV")
lookup$costPV = factor(lookup$costPV)

data_long2 <- gather(lookup, costB, BatSize, BC50:BC1000)

data_long2$costBat = as.character(lapply(strsplit(as.character(data_long2$costB), split="BC"), "[", 2))
data_long2$costB = NULL

data_long2$costPV = as.numeric(as.character(data_long2$costPV))
data_long2$costBat = as.numeric(as.character(data_long2$costBat))

data_long2 = data_long2[,c(3,1,2)]

data_long2$costPV[is.na(data_long2$costPV)] = -9999

data_long2$dist = (Bcost - data_long2$costBat)^2 + (PVcost - data_long2$costPV)^2
optBatSize = data_long2[which.min(data_long2$dist), "BatSize"]

}



if (run_lifetime = 't'){ # run lifetime module in Python


folder1 = "C:/GamsProject_CG/Replicate/RUNS/";
folder2=paste(folder1, "RUNS_B80eff_noDR/", sep="") };

# filenm = paste(folder2, "PV_B_DR_pv", as.character(optPVsize), 
#                "_Bc", as.character(optBatSize), 
#                "_DRc0_sum.csv", sep="");

filenm = paste(folder2, "PV_B_DR_pv", as.character(optPVsize), 
               "_Bc", as.character(optBatSize),
               "_DRc0_dis.csv", sep="");

batt_result = read.csv(filenm, header=TRUE)

cc_v2 = data.frame(matrix(vector(), 8760, 2, dimnames=list(c(), c("Hour","Battery.SOC"))), stringsAsFactors=F)
cc = batt_result[,c("Hour","Battery.SOC")]
cc = cc[order(cc$Hour),]
last_value = -99
for (i in 1:8760){
  if (cc$Battery.SOC[i] != last_value) {
    cc_v2$Hour[i] = cc$Hour[i]
    cc_v2$Battery.SOC[i] = cc$Battery.SOC[i]
    last_value = cc$Battery.SOC[i]
  }
}
cc_v2 = cc_v2[!is.na(cc_v2$Hour),]
tail(cc_v2, 10)

cc_v2$Battery.SOC.l1 = c(0, head(cc_v2$Battery.SOC, -1))
cc_v2$Battery.SOC.f1 = c(tail(cc_v2$Battery.SOC, -1), NA)
cc_v2$Battery.SOC.dsign = sign( (cc_v2$Battery.SOC - cc_v2$Battery.SOC.l1) * 
                                  (cc_v2$Battery.SOC.f1 - cc_v2$Battery.SOC) ) * 500

cc_v2$Battery.SOC.dsign[1] = -500

################## this is the result to pour into Python
cc_v3 = cc_v2[cc_v2$Battery.SOC.dsign<0,]
cc_v3$Battery.SOC[length(cc_v3$Battery.SOC)] = 0
tail(cc_v3, 10)

dir.create(file.path("C:/GamsProject_CG/Replicate/Lifetime/"), showWarnings=FALSE)
setwd(file.path("C:/GamsProject_CG/Replicate/Lifetime/"))
write.csv(cc_v3[,"Battery.SOC"], file="./SOC.csv", row.names=FALSE)

################## Call Python

a=system(paste("python test2.py", as.character(optBatSize), as.character(optBatSize), "-nocr=1"));


################## Calculate NPV for Battery
filenm = 'C:/GamsProject_CG/Replicate/Lifetime/life_Results.csv';
#filenm = '/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/Lifetime/life_Results.csv';
life_Results = read.csv(filenm, header = TRUE) 
years = life_Results[1,1]
life_Results$CapFadePct = life_Results[,length(life_Results)] / life_Results[1,length(life_Results)]


# Calculate NPV below
# http://www.mathepi.com/comp/discounting.html
# npv = sum(first_year_cash * (1-ann_CRF)^(1:(lt_year)))

NPV = as.data.frame(batt_result[,c("Hour", "EnergyPrice")])
NPV$batt = batt_result$B_discharging - batt_result$B_charging
NPV$batt_value = NPV$batt * NPV$EnergyPrice

NPV_rev = sum( sum(NPV$batt_value) * life_Results$CapFadePct[1:years] * (1-ann_CRF)^(1:years) )

source("C:/GamsProject_CG/Replicate/Final/mortgage.R")
mortgage(P=Bcost * optBatSize * 0.8, I=6, L=20, amort=T, plotData=F)  # 6% annual interest rate for loan
NPV_cost = Bcost * optBatSize * 0.2 + sum(aDFyear$Annual_Payment[1] * (1-0.1)^(1:20))  # 10% discount rate

NPV_value = NPV_rev - NPV_cost

NPV$netload = batt_result$Purchases - batt_result$Sales

} # End running lifetime module in Python



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
  install.packages("directlabels")  
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

# ggplot(a2, aes(x=Battery, y=PV, z = z)) +
#   stat_contour(aes(colour = ..level..), size = 1.5) +
#   geom_dl(aes(label=..level.., colour=..level..),
#                 data=a2, method="bottom.pieces", stat="contour") +
#   labs(x= "Battery Cost ($/kWh)",
#              y="PV Cost after incentives ($/kW)", 
#              title="Savings on Annual Electricity Costs (%)") + 
#   scale_x_discrete(labels=c(as.character(costB)), limits=c(as.character(costB))) +
#   scale_y_discrete(labels=c(as.character(costPV)), limits=c(as.character(costPV))) +
#   theme(axis.title = element_text(face="bold", colour="#000099", size=16),
#           axis.text.x  = element_text(face="bold", colour="#333333", angle=55, vjust=0.5, size=12),
#           axis.text.y  = element_text(face="bold", colour="#333333", angle=15, vjust=0.5, size=12),
#           plot.title = element_text(face="bold", colour="#000099", hjust=0, size=18),
#           panel.background = element_rect(fill = '#F8F8F8'),
#           panel.grid.major = element_line(colour = '#E0E0E0'),
#           legend.position="none")

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

# ggplot(a2, aes(x=Battery, y=PV, z = z)) + 
#   stat_contour(aes(colour = ..level..), size = 1.5) +
#   geom_dl(aes(label=..level.., colour=..level..),
#                 data=a2, method="bottom.pieces", stat="contour") +
#   labs(x= "Battery Cost ($/kWh)",
#              y="PV Cost after incentives ($/kW)", 
#              title="Optimal PV Capacity (kW)") +
#   scale_x_discrete(labels=c(as.character(costB)), limits=c(as.character(costB))) +
#   scale_y_discrete(labels=c(as.character(costPV)), limits=c(as.character(costPV))) +
#   theme(axis.title = element_text(face="bold", colour="#000099", size=16),
#           axis.text.x  = element_text(face="bold", colour="#333333", angle=55, vjust=0.5, size=12),
#           axis.text.y  = element_text(face="bold", colour="#333333", angle=15, vjust=0.5, size=12),
#           plot.title = element_text(face="bold", colour="#000099", hjust=0, size=18),
#           panel.background = element_rect(fill = '#F8F8F8'),
#           panel.grid.major = element_line(colour = '#E0E0E0'),
#           legend.position="none")

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





