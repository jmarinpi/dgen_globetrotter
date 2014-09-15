

* ----- GAMS Options & Title : -------------------------------------------------
$offlisting
option solprint = off, limrow = 0, limcol = 0, iterlim = 10000000, mip = cplex;
$offsymlist
$offsymxref

$title PV and Battery Optimization Model
*-------------------------------------------------------------------------------




* ---- SET OUTPUT FILES: -------------------------------------------------------
file
        battery_sum                /z_batt_DR_sum.csv/
        battery_dis                /z_batt_DR_dis.csv/;
*-------------------------------------------------------------------------------



*====== DECLARE FIXED SETS, SCALARS AND PARAMETERS: ============================
set
* --- T is the index for hours in a year, and iM is the index for months ----
      T    / 1 * 8760 /
      iM   / 1 * 12   /;


* --- Battery Details: ---------------------------------------------------
scalars
* # of hours of storage in the Battery (hours * kW of capacity)
     B_hr_storage

* min and max charging capacity of Battery (kW) *when charging or discharging*
     B_min_OP
     B_max_OP

* round trip efficiency
     B_rTrip_eff

* variable O&M cost of operating the battery in $/kWh
     operating_cost

* --- Scale PV generation: ------------------------------------------------
* scales the size of a PV system (# of kW of DC nameplate capacity)
     PVscale

* --- Net Metering: -------------------------------------------------------
* sets the maximum # of kWh that could be 'sold' back to the grid in any hour
*  through Net Metering
     eSoldCap
;

* Demand Response parameters;
*scalar    DReff    / 1 /;
*scalar    DRmin_OP / 0 /;
*scalar    DRmax_OP / 1 /;
*scalar    DRmax /  500 /;
*scalar    DRmin / -500 /;
*scalar    DRmax_ST_hr / 4 /;
*scalar    DRmax_DP_hr / -1 /;


scalars
* --- DEMAND RESPONSE VARIABLES: ---
* --- roundtrip efficiency of DR
           DReff
* --- DR operational min and max (thresholds to be in a charge or discharge state)
           DRmin_OP
           DRmax_OP
* --- DR power constraints (kW); max - discharge state, min - charge state
           DRmax
           DRmin
* --- number of hours (equivalent) of energy storage in charge and discharge
           DRmax_ST_hr
           DRmax_DP_hr
;


scalar
         avg_sales_price    / 0.15 / ;



parameters
* ----- Hourly Electricity Rate: --------------------------
        ePrice(T)
*               price of energy in each hour

* ----- Building Electricity Load - Hourly: ---------------
        eLoad(T)


* ----- Demand Schedules - hourly ($/kW): -----------------
* ----- two options: 1. hourly price ($/kW) for hours that apply
* -----              2. zero for hours that don't apply to that schedule
        demand1(T)
        demand2(T)
        demand3(T)

* ----- *need to change to PV data: -----------------------
        PV_gen(T)
*               generation available from wind farm in each hour


* ----- Month boundaries, in hours: -----------------------
        hrs_MonthL(iM)
        hrs_MonthH(iM)

;
*===============================================================================



* ===== R E A D   D A T A : ====================================================
* --- get battery input variables: ---
$include .\LOAD_DATA\MODEL_RUN_PARAMETERS_wDR.dat

* --- get hourly parameters: ---
$include .\LOAD_DATA\ELECTRICITY_PRICE.dat
$include .\LOAD_DATA\ELECTRICITY_LOAD.dat
$include .\LOAD_DATA\DEMAND_r1.dat
$include .\LOAD_DATA\DEMAND_r2.dat
$include .\LOAD_DATA\DEMAND_r3.dat

* --- get hours that separate months, to relate month boundaries to T
$include .\LOAD_DATA\HOURS_SEP_MONTHS.dat

* --- get hourly PV_gen(T) (normalized to 1 kW) ---
$include .\LOAD_DATA\PV_GEN.dat

* ==============================================================================



* ======  V A R I A B L E S    T O    S O L V E    F O R : =====================
positive variables
        PVused(T)
*               PV generation used in each hour
        PVcurtailed(T)
*               PV energy that is curtailed
        Battery_SOC(T)
*               storage level of CAES facility at end of each hour
        Battery_charged(T)
*               energy put into CAES in each hour
        Battery_discharged(T)
*               energy taken out of CAES in each hour
        eBought(T)
*               energy purchased in each hour from the Utility
        eSold(T)
*               energy 'sold' back to the utility in each hour thru Net Metering
        dCost1(iM)
*               monthly demand cost for hours in 1st block
        dCost2(iM)
*               monthly demand cost for hours in 2nd block
        dCost3(iM)
*               monthly demand cost for hours in 3rd block

        DR_SOC(T)
*               storage level of CAES facility at end of each hour
        DR_charged(T)
*               energy put into CAES in each hour
        DR_discharged(T)
*               energy taken out of CAES in each hour

;

binary variable
        discharging(T)
*               binary variable indicating the battery is discharging in each hour
        discharge_start(T)
*               binary variable indicating the battery started discharging
        charging(T)
*               binary variable indicating the battery is charging in each hour
        charge_start(T)
*               binary variable indicating the battery started charging
        allow_sales(T)
*               binary variable indicating hours where PV generation > load
        DR_charging(T)
*               bv indicating that the DR resource is charging
        DR_discharging(T)
*               bv indicating that the DR resource is discharging

;

free variable
        net_cost
*               net annual cost of electricity
;
*===============================================================================





* ======  D E C L A R E     E Q U A T I O N S :  ===============================
equations
* Objective function
        net_cost_def
* Constraints
        Battery_SOC_def(T)
        Energy_balance(T)
        PV_balance(T)
        discharge_lower_bound(T)
        discharge_upper_bound(T)
        charge_lower_bound(T)
        charge_upper_bound(T)
        discharge_start_const(T)
        charge_start_const(T)
        no_charge_discharge_same_time(T)
        Demand_cost1(T,iM)
        Demand_cost2(T,iM)
        Demand_cost3(T,iM)
        Sales_constraint(T)
        DR_SOC_def(T)
        DR_discharge_lower_bound(T)
        DR_discharge_upper_bound(T)
        DR_charge_lower_bound(T)
        DR_charge_upper_bound(T)
        DR_no_charge_discharge_same(T)
*        DR_energy(T)

;
*===============================================================================




* ========  E Q U A T I O N S : ================================================


* --- O P T I M I Z A T I O N    F U N C T I O N : -----------------------------
net_cost_def..
        net_cost =e= sum(T, eBought(T)*ePrice(T))
                  +  sum(T, operating_cost*Battery_discharged(T))
                  +  sum(iM, dCost1(iM) + dCost2(iM) + dCost3(iM))
                  -  sum(T, eSold(T)*avg_sales_price);

*-------------------------------------------------------------------------------

* --- Sum Demand Costs for each Month and for each Demand time 'bin': ----------------------------------
Demand_cost1(T,iM)..
        dCost1(iM) =g= (demand1(T)*eBought(T))$( ord(T)>hrs_MonthL(iM) and ord(T)<=hrs_MonthH(iM));

Demand_cost2(T,iM)..
        dCost2(iM) =g= (demand2(T)*eBought(T))$( ord(T)>hrs_MonthL(iM) and ord(T)<=hrs_MonthH(iM));

Demand_cost3(T,iM)..
        dCost3(iM) =g= (demand3(T)*eBought(T))$( ord(T)>hrs_MonthL(iM) and ord(T)<=hrs_MonthH(iM));
*-------------------------------------------------------------------------------

* --- Track Battery State-of-Charge (SOC): -------------------------------------
Battery_SOC_def(T)..
        Battery_SOC(T) =e= Battery_SOC(T-1)$(ord(T) >= 2)
                           + (Battery_charged(T) - Battery_discharged(T));
*-------------------------------------------------------------------------------


* --- Make sure load is met in all hours: --------------------------------------
Energy_balance(T)..
        eLoad(T) =e= eBought(T) + PVused(T) - eSold(T)
                    + Battery_discharged(T)*B_rTrip_eff  - Battery_charged(T)
                    + DR_discharged(T) - DR_charged(T);
*-------------------------------------------------------------------------------


* --- Demand response energy constraint: ---------------------------------------
*DR_energy(T)..
*        DR(T) + DR(T-2)$(ord(T)>2) + DR(T-1)$(ord(T)>1)
*              + DR(T+2)$(ord(T)<8759) + DR(T+1)$(ord(T)<8760) =e= 0;

*DR_energy(T)..
*        DR(T) =e= DR(T-2)$(ord(T)>2)    + DR(T-1)$(ord(T)>1)
*                + DR(T+2)$(ord(T)<8759) + DR(T+1)$(ord(T)<8760);
*-------------------------------------------------------------------------------

* --- PV can be spilled if: SOC is full & PV_gen(T) > eLoad(T) -----------------
PV_balance(T)..
        PVused(T) + PVcurtailed(T) =e= PV_gen(T)*PVscale;
*-------------------------------------------------------------------------------


* --- Set lower and upper bounds on charging / discharging: --------------------
discharge_lower_bound(T)..
        Battery_discharged(T)*B_rTrip_eff =g= B_min_OP * discharging(T);

discharge_upper_bound(T)..
        Battery_discharged(T)*B_rTrip_eff =l= B_max_OP * discharging(T);

charge_lower_bound(T)..
        Battery_charged(T) =g= B_min_OP * charging(T);
charge_upper_bound(T)..
       Battery_charged(T) =l= B_max_OP * charging(T);
*-------------------------------------------------------------------------------


* --- diagnose when battery starts charging or discharging: --------------------
discharge_start_const(T)..
        discharge_start(T) =g= discharging(T) - discharging(T-1)$(ord(T) >= 2);

charge_start_const(T)..
        charge_start(T) =g= charging(T) - charging(T-1)$(ord(T) >= 2);
*-------------------------------------------------------------------------------


* --- Don't allow simultaneous charging and discharging: -----------------------
no_charge_discharge_same_time(T)..
         charging(T) + discharging(T) =l= 1.5;
*-------------------------------------------------------------------------------


* --- PV can only be sold if PV_gen(T) > eLoad(T): -----------------------------
Sales_constraint(T)..
       eSold(T) =l= (PV_gen(T)*PVscale - eLoad(T))*allow_sales(T);

*-------------------------------------------------------------------------------







*  ------------  DEMAND RESPONSE: ----------------------------------------------

* --- Track DEMAND RESPONSE State-of-Charge (SOC): ----------------------------
DR_SOC_def(T)..
        DR_SOC(T) =e= DR_SOC(T-1)$(ord(T) >= 2)
                           + (DR_charged(T) - DR_discharged(T)/DReff);
*-------------------------------------------------------------------------------



* --- Set lower and upper bounds on charging / discharging: --------------------
DR_discharge_lower_bound(T)..
        DR_discharged(T) =g= DRmin * DR_discharging(T);
DR_discharge_upper_bound(T)..
        DR_discharged(T) =l= DRmax * DR_discharging(T);

DR_charge_lower_bound(T)..
        DR_charged(T) =g= DRmin * DR_charging(T);
DR_charge_upper_bound(T)..
        DR_charged(T) =l= DRmax * DR_charging(T);
*-------------------------------------------------------------------------------


* --- diagnose when battery starts charging or discharging: --------------------
*DR_discharge_start_const(T)..
*        DR_discharge_start(T) =g= DR_discharging(T) - DR_discharging(T-1)$(ord(T) >= 2);

*DR_charge_start_const(T)..
*        DR_charge_start(T) =g= DR_charging(T) - DR_charging(T-1)$(ord(T) >= 2);
*-------------------------------------------------------------------------------


* --- Don't allow simultaneous charging and discharging: -----------------------
DR_no_charge_discharge_same(T)..
         DR_charging(T) + DR_discharging(T) =l= 1.5;
*-------------------------------------------------------------------------------






*-------------------------------------------------------------------------------

*===============================================================================



* =====   S E T U P     T H E     M O D E L : ==================================
model PV_battery /all/;

PV_battery.optfile = 1;


* --- ADD VARIABLE BOUNDS: ---
Battery_SOC.up(T) = B_max_OP * B_hr_storage / B_rTrip_eff;
Battery_charged.up(T) = B_max_OP;
Battery_discharged.up(T) = B_max_OP * B_rTrip_eff;
eSold.lo(T) = 0;
*eSold.up(T) = 0;
eSold.up(T) = eSoldCap;
DR_SOC.lo(T) = DRmax_DP_hr*DRmax;
DR_SOC.up(T) = DRmax_ST_hr*DRmax;


* --- SOLVE PARAMETERS: ---
solve PV_battery using mip minimizing net_cost;

* ==============================================================================



* ======  W R I T E   O U T P U T : ============================================
if((PV_battery.modelstat=1 or PV_battery.modelstat=2 or PV_battery.modelstat=8),
*       Export results
        put battery_sum;

                put '" ",' /;
                put '"Electricity Load",' /;
                put '"Annual Load (kWh)",' sum(T, eLoad(T))/;

                put '" ",' /;
                put '"Annual PV used",' /;
                put '"PV available (kWh)",' sum(T, PV_gen(T)*PVscale)/;
                put '"PV used (kWh)",' sum(T, PVused.l(T))/;
                put '"PV spilled (kWh)",' sum(T, PVcurtailed.l(T))/;
                put '"PV sold - NEM (kWh)", ' sum(T, eSold.l(T))/;

                put '" ",' /;
                put '"Storage",' /;
                put '"Total energy stored (kWh)",' sum(T, Battery_charged.l(T))/;
                put '"Batt charge (hrs)",' sum(T$(Battery_charged.l(T)>0), 1)/;
                put '"Batt discharge (hrs)",' sum(T$(Battery_discharged.l(T)>0), 1)/;

                put '" ",' /;
                put '"Demand Response",' /;
                put '"DR in (hrs)",' sum(T$(DR_charged.l(T)>0), 1)/;
                put '"DR out (hrs)",' sum(T$(DR_discharged.l(T)>0), 1)/;

                put '" ",' /;
                put '"Annual Costs",' /;
                put '"Energy Cost ($)",' sum(T, eBought.l(T)*ePrice(T))/;
                put '"Demand p1 Cost ($)",' sum(iM, dCost1.l(iM))/;
                put '"Demand p2 Cost ($)",' sum(iM, dCost2.l(iM))/;
                put '"Demand p3 Cost ($)",' sum(iM, dCost3.l(iM))/;
                put '"Annual Demand Cost ($)",' sum(iM, dCost1.l(iM) + dCost2.l(iM) + dCost3.l(iM))/;
                put '"Operating Cost ($)",' sum(T, operating_cost*Battery_discharged.l(T))/;
                put '"Sales Rev - NEM  ($)",' sum(T, -1*eSold.l(T)*ePrice(T))/;
                put '"Total Costs ($)",' net_cost.l /;

                put '" ",' /;
                 put '" ",' /;
                put '"Profit",' /;
                put '"Net Cost",' net_cost.l/;


        put battery_dis;
        put '"Hour", "EnergyPrice", "Load", "Purchases", "Sales", "PV used","PV curtailed", "DR_SOC", "Battery SOC","Charging","Discharging", "Discharge", "Charge", "Discharge Start", "Charge Start"'/;
        battery_dis.pc = 5;
        loop(T,
                put ord(T), ePrice(T), eLoad(T), eBought.l(T), eSold.l(T), PVused.l(T), PVcurtailed.l(T), DR_SOC.l(T), Battery_SOC.l(T), Battery_charged.l(T), Battery_discharged.l(T), discharging.l(T), charging.l(T), discharge_start.l(T), charge_start.l(T)/;
        );
);
*===============================================================================


* AND WE'RE DONE!