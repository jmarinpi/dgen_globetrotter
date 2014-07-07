-- Function: wind_ds.scoe(numeric, numeric, numeric, numeric, numeric, numeric, double precision, numeric, text, numeric, numeric)
--DROP FUNCTION wind_ds.scoe(numeric, numeric, numeric, numeric, numeric, numeric, double precision, numeric, text, numeric, numeric);

SET ROLE 'server-superusers';
CREATE OR REPLACE FUNCTION wind_ds.scoe(ic numeric, fom numeric, vom numeric, naep numeric, cap numeric, ann_elec_cons numeric, nem_system_limit_kw double precision, excess_generation_factor numeric, nem_availability text, oversize_factor numeric DEFAULT 1.15, undersize_factor numeric DEFAULT 0.5)
  RETURNS double precision AS
$BODY$

    """ Calculate simple metric for evaluating optimal capacity-height among several
        possibilities. The metric does not caclulate value of incentives, which are 
        assumed to scale btw choices. In sizing, allow production to exceed annual 
        generation by default 15%, and undersize by 50%.
        
       IN:
           ic  - Installed Cost ($/kW)
           fom - Fixed O&M ($/kW-yr)
           vom - Variable O&M ($/kWh)
           naep - Annual Elec Production (kWh/kw/yr)
           cap - Proposed capacity (kW)
           ann_elec_cons - Annual Electricity Consumption (kWh/customer/yr)
           nem_system_limit_kw - size in kW of NEM policy
           excess_generation_factor - % of annual generation that exceed load on a moment-to-moment basis
           nem_availability - string indiciating input switch NEM policy
           oversize_factor - Severe penalty for  proposed capacities whose aep exceed
                             annual electricity consumption by 15% (default)
           undersize_factor - Small penalty for proposed capacities whose aep is beneath
                             annual electricity consumption by 50% (default)
       
       OUT:
           scoe - numpy array - simple lcoe (lower is better)
    """

    if nem_availability == 'Full Net Metering Everywhere':
       percent_of_gen_monetized = 1
    elif nem_availability == 'Partial - Avoided Cost':
        percent_of_gen_monetized = 1 - 0.5 * excess_generation_factor # Assume avoided cost is roughly 50% of retail, this effectively halves excess_gen_factor
    elif nem_availability == 'Partial - No Outflows':
        percent_of_gen_monetized = 1 - excess_generation_factor
    elif nem_availability == 'No Net Metering Anywhere':
        percent_of_gen_monetized = 0
    
    if nem_system_limit_kw >= cap and nem_availability != 'No Net Metering Anywhere':
        percent_of_gen_monetized = 1
        
    if naep == 0:
        return float('inf')
    else:
        scoe = (ic + 30 * fom + 30 * naep * vom) / (30 * naep) # $/kWh
        # add in a penalty for oversizing that scales with the degree of oversizing
        oversized = ((naep * cap / ann_elec_cons) > (percent_of_gen_monetized * oversize_factor)) * ((naep * cap / ann_elec_cons) / (percent_of_gen_monetized * oversize_factor))
        undersized = ((naep * cap / ann_elec_cons) < (percent_of_gen_monetized * undersize_factor)) / ((naep * cap / ann_elec_cons) / (percent_of_gen_monetized * undersize_factor))
        scoe = scoe + oversized * 10 + undersized * 0.1 # Penalize under/over sizing    
         
        return scoe

  $BODY$
  LANGUAGE plpythonu STABLE
  COST 100;
ALTER FUNCTION wind_ds.scoe(numeric, numeric, numeric, numeric, numeric, numeric, double precision, numeric, text, numeric, numeric)
  OWNER TO "server-superusers";
