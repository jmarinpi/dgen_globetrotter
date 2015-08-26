

#---- load packages: --------
library(dplyr)


# --- design run: ----------
read_eLoad_PV = 'f'; # read SAM data
calc_demand = 'f'; # check to see that you can replicate electricity costs (understand the rules)
write_OUTPUT = 't'; # write data in a format that GAMS will accept

#---------------------------






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
  readFile="C:\\Users\\edrury\\Dropbox\\AA_OPTIMIZATION\\GAMS_battery\\LOAD_DATA\\LongBeach_eLoad_PV.csv";   # global
  data = read.csv(readFile)
  names(data)<-c('eLoad','eCost', 'TOU_e', 'TOU_d', 'ePV');
  df_hrly=mutate(  select(data, 
                          ePV,
                          eLoad, 
                          eCost,
                          TOU_e,
                          TOU_d),
                 rTOU_e = TOU_en[data$TOU_e],
                 rTOU_d = TOU_dm[data$TOU_d],
                 mn=mn_vector,
                 dy=dy_vector,
                 hr=hr_vector);

  # look at impact of demand based rates 
  y=df_hrly$eCost;
  f_demand = sum(y[hrs])/ sum(y);
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
};
#-------------------------------------------------------------------------------------------







#----- WRITE FORMATTED OUTPUT: -------------------------------------------------------------
if (write_OUTPUT == 't'){

#--------------------------------------------------
# Main difference from matlab is that multiple calls can go in one cat() statement, and you
# need to use the append flag for multiple cat() calls that you don't want to overwrite.
#--------------------------------------------------




# --- Hourly Electricity Prices: ----------------
filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/ELECTRICITY_PRICE.dat";
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
filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/ELECTRICITY_LOAD.dat";
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
filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/DM_SCHED.dat";
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
filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/DEMAND_r1.dat";
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
filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/DEMAND_r2.dat";
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
filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/DEMAND_r3.dat";
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
filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/PV_GEN.dat";
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
filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/HOURS_SEP_MONTHS.dat";
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





# --- Defining Model Inputs: -------------------------------------

B_hr_storage = 6;     # number of hours of storage
B_min_OP = 0;         # min storage to be in 'operating' mode
B_max_OP = 10;        # storage capacity (kW)
B_rTrip_eff = 1;      # round trip efficiency (<=100%)
operating_cost = 0;   # variable operating cost ($/kWh)
PVscale = 1000;      # size of PV system (kW)
eSoldCap = 1000;      # maximum amount of energy that can be net metered in a given hour (kW)


filenm="C:/Users/edrury/Dropbox/AA_OPTIMIZATION/GAMS_battery/LOAD_DATA/MODEL_RUN_PARAMETERS.dat";

cat("scalars \r\n",
    "\t B_hr_storage \t \r\n \t\t\t", " / ", toString(B_hr_storage), " / ", "\r\n", file=filenm, sep="")

cat("\t B_min_OP \t \r\n \t\t\t", " / ", toString(B_min_OP), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t B_max_OP \t \r\n \t\t\t", " / ", toString(B_max_OP), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t B_rTrip_eff \t \r\n \t\t\t", " / ", toString(B_rTrip_eff), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t operating_cost \t \r\n \t\t\t", " / ", toString(operating_cost), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t PVscale \t \r\n \t\t\t", " / ", toString(PVscale), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat("\t eSoldCap \t \r\n \t\t\t", " / ", toString(eSoldCap), " / ", "\r\n", file=filenm, sep="", append=TRUE)
cat(";", file=filenm, sep="", append=TRUE); 

#---------------------------------------------------------------


system("C://Users//edrury//Dropbox//AA_OPTIMIZATION//GAMS_battery//battery_optimize_CALL_from_R.gms");






}; # ---------- END WRITE DATA: ---------------
#-------------------------------------------------------------------------------------------

