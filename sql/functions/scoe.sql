-- DROP FUNCTION wind_ds.scoe()
SET ROLE 'server-superusers';
CREATE OR REPLACE FUNCTION wind_ds.scoe(ic numeric, fom numeric, cap numeric, aec numeric, oversize_factor numeric default 1.15, undersize_factor numeric default 0.5)
  RETURNS numeric[] AS
  $BODY$

    """ Calculate simple metric for evaluating optimal capacity-height among several
        possibilities. The metric does not caclulate value of incentives, which are 
        assumed to scale btw choices. In sizing, allow production to exceed annual 
        generation by default 15%, and undersize by 50%.
        
       IN:
           ic  - Installed Cost ($/kW)
           fom - Fixed O&M ($/kW-yr)
           vom - Variable O&M ($/kWh)
           aep - Annual Elec Production (kWh/yr)
           cap - Proposed capacity (kW)
           aec - Annual Electricity Consumption (kWh/yr)
           oversize_factor - Severe penalty for  proposed capacities whose aep exceed
                             annual electricity consumption by 15% (default)
           undersize_factor - Small penalty for proposed capacities whose aep is beneath
                             annual electricity consumption by 50% (default)
       
       OUT:
           scoe - numpy array - simple lcoe (lower is better)
    """
    
    scoe = (ic + 30 * fom + 30 * aep * vom) / (30 * aep) # $/kWh
    oversized = (aep * cap / ann_elec_cons) > oversize_factor
    undersized = (aep * cap / ann_elec_cons) < undersize_factor
    scoe = scoe + oversized * 10 + undersized * 0.1 # Penalize under/over sizing    
     
    return scoe

  $BODY$
  LANGUAGE plpythonu stable
  COST 100;
RESET ROLE;

