    SELECT a.year, 
	   a.efficiency_improvement_factor,
	   a.density_w_per_sqft,
	   a.derate,
	   a.inverter_lifetime_yrs,
	   b.capital_cost_dollars_per_kw, 
	   b.inverter_cost_dollars_per_kw, 
	   b.fixed_om_dollars_per_kw_per_yr, 
	   b.variable_om_dollars_per_kwh, 
	   b.sector,
	   b.source as cost_projection_source,
	c.census_division_abbr,
	c.sector,
	c.escalation_factor as rate_escalation_factor,
	c.source as rate_escalation_source,
	d.scenario as load_growth_scenario,
	d.load_multiplier,
	e.carbon_dollars_per_ton
    FROM diffusion_solar.solar_performance_improvements a
    LEFT JOIN diffusion_solar.cost_projections_to_model b
    ON a.year = b.year
    LEFT JOIN diffusion_solar.rate_escalations_to_model c
    ON a.year = c.year
    LEFT JOIN diffusion_shared.aeo_load_growth_projections d
    ON c.census_division_abbr = d.census_division_abbr
    AND a.year = d.year
    LEFT JOIN diffusion_solar.market_projections e
    ON a.year = e.year
    WHERE a.year BETWEEN 2014 AND 2050
    AND c.sector in ('Residential');

-- final naep = naep for a given derate * improvement in efficiency * cumulative degradation