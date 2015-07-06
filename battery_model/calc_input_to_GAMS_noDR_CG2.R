

#---- load packages: --------
library(dplyr)

# --- design run: ----------
read_eLoad_PV = 'f'; # read SAM data
calc_demand = 'f'; # check to see that you can replicate electricity costs (understand the rules)
write_OUTPUT = 'f'; # write data in a format that GAMS will accept
run_model = 't'; # run GAMS model
#---------------------------

# --- FUNCTIONS: -------------------------------------------------------------------------

# ----  create file rename function: -----------------------------------
my.file.rename <- function(from, to) {
  todir <- dirname(to)
  if (!isTRUE(file.info(todir)$isdir)) dir.create(todir, recursive=TRUE)
  file.rename(from = from,  to = to)
}
#-----------------------------------------------------------------------



# --- end FUNCTIONS: ---------------------------------------------------------------------




# ---- READ SAM output data: -------------------------------------------------------------
if (read_eLoad_PV == 't'){

# prepare month, day, hr vectors
  # -- days and hours in each month: --
  days=c(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
  hrs = vector(mode="numeric", length=12);
  for (i in 1:12) {hrs[i] = sum(days[1:i])*24};
  hrs2=c(0, hrs[1:11]);
  # -- month, day, hr vectors: --
  hr_vector = rep(c(1:24),365);
  dy_vector = numeric();
  mn_vector = numeric();
  for (i in 1:12){ 
    mn_vector=c(mn_vector, rep(i, days[i]*24))
    for (j in 1:days[i]){dy_vector=c(dy_vector, rep(j, 24))}
  };
  
  
  # Long Beach commercial electricity rates (from SAM v.2014_1_14)
  TOU_en = c(0.0725, 0.0915, 0.0679, 0.1372, 0.3365); # $/kWh
  TOU_dm = c(11.11, 5.02, 17.13); # $/kW  

  # read data
  readFile="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/LongBeach_eLoad_PV.csv";
  # readFile="C:\\Users\\edrury\\Dropbox\\AA_OPTIMIZATION\\GAMS_battery\\LOAD_DATA\\LongBeach_eLoad_PV.csv";   # global  
  data = read.csv(readFile)
  names(data)<-c('eLoad','eCost', 'TOU_e', 'TOU_d', 'ePV');
  df_hrly=mutate(  select(data, 
                          ePV,
                          eLoad, 
                          eCost,
                          TOU_e,
                          TOU_d),
                 rTOU_e = TOU_en[data$TOU_e],   # from tiers to actual energy price
                 rTOU_d = TOU_dm[data$TOU_d],
                 mn=mn_vector,
                 dy=dy_vector,
                 hr=hr_vector);

  # look at impact of demand based rates 
  y=df_hrly$eCost;
  f_demand = sum(y[hrs])/ sum(y);
#  df_hrly$test = df_hrly$eLoad * df_hrly$rTOU_e       # This is right
} # finished reading hourly data
#-------------------------------------------------------------------------------------------







#------- CALCULATE MONTHLY DEMAND RATES (to understand the rules): ------------------------------
if (calc_demand == 't'){
dm=matrix(0, 12, 5);
for (i1 in 1:12){
for (i2 in 1:5){
  dm1=filter(df_hrly, 
             mn == i1, 
             TOU_d == i2); # drop empty rows columns
  d=dim(dm1); 
  if (d[1]>0 & i2<4){dm[i1,i2]=max(dm1$eLoad)*TOU_dm[i2]}; # demand charges in each bin
  if (i2 == 4) { dm[i1,i2]=df_hrly$eLoad[hrs[i1]]*df_hrly$rTOU_e[hrs[i1]]} # electricity charge
  if (i2 == 5) { dm[i1,i2]=444.8} # monthly fixed charge
}
};
dm=as.data.frame(dm);
names(dm)=c('d1', 'd2', 'd3', 'eRate', 'fixedCharge');

dm$ebill = rowSums(dm)
dm$verify = -y[hrs]  # check these two columns to see if they match

};
#-------------------------------------------------------------------------------------------






#----- WRITE FORMATTED OUTPUT: -------------------------------------------------------------
if (write_OUTPUT == 't'){

# ** NEED TO PUT THESE IN A WRITE FUNCTION, SO THE CODE IS CLEAN HERE ON JUST DEFINING
# DATA INPUT PARAMETERS;   ALSO SHOULD STARTE WITH BATTERY PARAMETERS
  
  
#--------------------------------------------------
# Main difference from matlab is that multiple calls can go in one cat() statement, and you
# need to use the append flag for multiple cat() calls that you don't want to overwrite.
#--------------------------------------------------




# --- Hourly Electricity Prices: ----------------
filenm="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/ELECTRICITY_PRICE.dat";
cat("parameter \r\n",
    "\t ePrice(T) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="");

for (i in 1:8760){
  cat("\t\t", toString(i), "\t", toString(df_hrly$rTOU_e[i]), "\r\n", 
      file=filenm, sep="", append=TRUE)
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", file=filenm, sep="", append=TRUE);
#--------------------------------------------



# --- Hourly Electricity Load: ----------------
filenm="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/ELECTRICITY_LOAD.dat";
cat("parameter \r\n",
    "\t eLoad(T) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="");

for (i in 1:8760){
  cat("\t\t", toString(i), "\t", toString(df_hrly$eLoad[i]), "\r\n", 
      file=filenm, sep="", append=TRUE)
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", file=filenm, sep="", append=TRUE);
#--------------------------------------------



# --- Demand Schedule: ----------------
# This is demand charges that vary with time
filenm="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/DM_SCHED.dat";
cat("parameter \r\n",
    "\t dm_sched(T) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="");

for (i in 1:8760){
  cat("\t\t", toString(i), "\t", toString(df_hrly$TOU_d[i]), "\r\n", 
      file=filenm, sep="", append=TRUE)
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", file=filenm, sep="", append=TRUE);
#--------------------------------------------



# --- Demand Rate 1: ----------------
filenm="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/DEMAND_r1.dat";
cat("parameter \r\n",
    "\t demand1(T) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="");

for (i in 1:8760){
if(df_hrly$TOU_d[i] == 1){
  cat("\t\t", toString(i), "\t", toString(df_hrly$rTOU_d[i]), "\r\n", 
      file=filenm, sep="", append=TRUE)}
  else {
  cat("\t\t", toString(i), "\t", toString(0), "\r\n", 
      file=filenm, sep="", append=TRUE)}  
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", file=filenm, sep="", append=TRUE);
#--------------------------------------------


# --- Demand Rate 2: ----------------
filenm="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/DEMAND_r2.dat";
cat("parameter \r\n",
    "\t demand2(T) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="");

for (i in 1:8760){
  if(df_hrly$TOU_d[i] == 2){
    cat("\t\t", toString(i), "\t", toString(df_hrly$rTOU_d[i]), "\r\n", 
        file=filenm, sep="", append=TRUE)}
  else {
  cat("\t\t", toString(i), "\t", toString(0), "\r\n", 
      file=filenm, sep="", append=TRUE)}  
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", file=filenm, sep="", append=TRUE);
#--------------------------------------------


# --- Demand Rate 3: ----------------
filenm="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/DEMAND_r3.dat";
cat("parameter \r\n",
    "\t demand3(T) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="");

for (i in 1:8760){
  if(df_hrly$TOU_d[i] == 3){
    cat("\t\t", toString(i), "\t", toString(df_hrly$rTOU_d[i]), "\r\n", 
        file=filenm, sep="", append=TRUE)}
  else {
  cat("\t\t", toString(i), "\t", toString(0), "\r\n", 
      file=filenm, sep="", append=TRUE)}  
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", file=filenm, sep="", append=TRUE);
#--------------------------------------------




# --- PV Generation: ----------------
filenm="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/PV_GEN.dat";
cat("parameter \r\n",
    "\t PV_gen(T) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="");

for (i in 1:8760){
  cat("\t\t", toString(i), "\t", toString(df_hrly$ePV[i]), "\r\n", 
      file=filenm, sep="", append=TRUE)
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", file=filenm, sep="", append=TRUE);
#--------------------------------------------



# --- Defining Month boundaries as # of hours: ----------------
filenm="/Users/cdong/Documents/CGDong/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/HOURS_SEP_MONTHS.dat";
cat("parameter \r\n",
    "\t hrs_MonthL(iM) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="");

for (i in 1:12){
  cat("\t\t", toString(i), "\t", toString(hrs2[i]), "\r\n", 
      file=filenm, sep="", append=TRUE)
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", "\r\n", "\r\n", "\r\n", file=filenm, sep="", append=TRUE);

cat("parameter \r\n",
    "\t hrs_MonthH(iM) \r\n",
    "\t\t\t", " / ", "\r\n", file=filenm, sep="", append=TRUE);

for (i in 1:12){
  cat("\t\t", toString(i), "\t", toString(hrs[i]), "\r\n", 
      file=filenm, sep="", append=TRUE)
};

cat("\t\t\t", " / ", "\r\n", 
    ";", "\r\n", file=filenm, sep="", append=TRUE);
#---------------------------------------------------------------


};# ---------- END WRITE DATA: ---------------







#-----  RUN MODEL: ----------------------------------------------
if (run_model == 't'){

vPV = seq(from=0.5, to=1, by=0.05); # PV offset percentages
#vPV = seq(from=0, to=2000, by=200); # PV size in kW
vB  = seq(from=0, to=16000, by=400); # battery size in kWh
vDR = seq(from=0, to=0, by=400); # DR size in kw
#vB2 = seq(from=0, to=5, by=1); # total hours of Battery storage

PVscale0 = 2000;

###############################################################  Careful - this gives 11*11*8 = 968 runs!

Ctimer = array(0, c(length(vPV),1));
for (i1 in 1:length(vPV)){
for (i2 in 1:length(vB)){
for (i3 in 1:length(vDR)){
  
# --- Defining Model Inputs: -------------------------------------
B_min_OP = 0;         # min storage to be in 'operating' mode
B_max_OP = vB[i2];      # storage capacity (kWh)
B_rTrip_eff = .8;      # round trip efficiency of the battery (<=1)
operating_cost = 0;   # variable operating cost ($/kWh)
PVscale = PVscale0*vPV[i1];    #1000; # size of PV system (kW)
eSoldCap = 1000;      # maximum amount of energy that can be net metered in a given hour (kW)
DReff = 1;            # round trip efficiency of DR (<=1)
DRmin_OP = 0;         # minimum operating mode (0 to 1)
DRmax_OP = 1;         # maximum operating mode (0 to 1)
DRmax    = vDR[i3];       # maximum power output, comparable to generation (kW)
DRmin    = -1*vDR[i3];      # maximum power input, comparable to storage (kW)
DRmax_ST_hr = 6;       # number of hours of storage in a 'charged' state, e.g. thermal storage like chilled water
DRmax_DP_hr = -1;     # number of hours of depleted energy in a 'discharged' state; e.g. warmer temps in summer

filenm="C:/GamsProject_CG/Replicate/LOAD_DATA/MODEL_RUN_PARAMETERS_woDR2.dat";

cat("scalars \r\n",
    "\t B_min_OP \t \r\n \t\t\t", " / ", toString(B_min_OP), " / ", "\r\n", file=filenm, sep="")

cat("\t B_max_OP \t \r\n \t\t\t", " / ", toString(B_max_OP), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t B_rTrip_eff \t \r\n \t\t\t", " / ", toString(B_rTrip_eff), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t operating_cost \t \r\n \t\t\t", " / ", toString(operating_cost), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t PVscale \t \r\n \t\t\t", " / ", toString(PVscale), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t eSoldCap \t \r\n \t\t\t", " / ", toString(eSoldCap), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t DReff \t \r\n \t\t\t", " / ", toString(DReff), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t DRmin_OP \t \r\n \t\t\t", " / ", toString(DRmin_OP), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t DRmax_OP \t \r\n \t\t\t", " / ", toString(DRmax_OP), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t DRmin \t \r\n \t\t\t", " / ", toString(DRmin), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t DRmax \t \r\n \t\t\t", " / ", toString(DRmax), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t DRmax_ST_hr \t \r\n \t\t\t", " / ", toString(DRmax_ST_hr), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t DRmax_DP_hr \t \r\n \t\t\t", " / ", toString(DRmax_DP_hr), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat(";", file=filenm, sep="", append=TRUE); 

#---------------------------------------------------------------


t0=proc.time();  # start clock

setwd('C:/GamsProject_CG/Replicate/')
#setwd('C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/')

# call the GAMS model
a=system("C:/GAMS/win64/24.1/gams.exe battery_optimize_W_DR2_noDR2.gms -nocr=1");
#a=system("gams battery_optimize_W_DR2_eSalesAvg.gms -nocr=1");

# remove the GAMS directory
unlink("225*", recursive = TRUE);

t1=proc.time();

folder1=("C:/GamsProject_CG/Replicate/");
folder2=("C:/GamsProject_CG/Replicate/RUNS/RUNS_B80eff_noDR/");

# move files (my.file.rename is a function defined above)
my.file.rename(from = paste(folder1, "z_batt_DR_dis.csv", sep=""),
               to = paste(folder2, "PV_B_DR_pv", as.character(vPV[i1]), 
               "_Bc", as.character(vB[i2]), 
               "_DRc", as.character(vDR[i3]), "_dis.csv", sep=""));
my.file.rename(from = paste(folder1, "z_batt_DR_sum.csv", sep=""),
               to = paste(folder2, "PV_B_DR_pv", as.character(vPV[i1]), 
               "_Bc", as.character(vB[i2]), 
               "_DRc", as.character(vDR[i3]), "_sum.csv", sep=""));

t2=proc.time();

print(paste(" . . PV ", as.numeric(vPV[i1]),
            " -Bcap- ", as.numeric(vB[i2]), 
            " -DRcap- ", as.numeric(vDR[i3]),
            " -time- ", as.numeric(t2-t0)[3], 
            " . . ", sep=""));


Ctimer[i1] = as.numeric(t2-t0)[3]; # get total computational time in seconds

}; # ---- end DR size loop: --------
}; # ---- end  B size loop: --------
}; # ---- end PV size loop: --------


}; # ---------- END RUN MODEL: ---------------
#-------------------------------------------------------------------------------------------
