set role 'diffusion-writers';

DROP VIEW IF EXISTS diffusion_results_2016_05_31_11h22m59s.temporal_factors_wind;
CREATE VIEW diffusion_results_2016_05_31_11h22m59s.temporal_factors_wind AS
SELECT -- general identifiers
	'solar'::VARCHAR(5) as tech,
	a.year,
	g.state_abbr,
	e.census_division_abbr,
	-- costs
	a.sector_abbr,
	a.installed_costs_dollars_per_kw,
	a.fixed_om_dollars_per_kw_per_yr,
	a.variable_om_dollars_per_kwh,
	-- cost adjusters
	b.size_adjustment_factor,
	b.base_size_kw,
	b.new_construction_multiplier,
	-- performance
	c.efficiency_improvement_factor,
	c.density_w_per_sqft,
	c.inverter_lifetime_yrs,
	-- load growth
	e.scenario as load_growth_scenario,
	e.load_multiplier,
	-- carbon tax 
	f.carbon_dollars_per_ton,
	h.t_co2_per_kwh,
	h.t_co2_per_kwh * 100 * f.carbon_dollars_per_ton as carbon_price_cents_per_kwh, 
	-- leasing availability
	i.leasing_allowed

FROM diffusion_results_2016_05_31_11h22m59s.input_solar_cost_projections_to_model a
LEFT JOIN diffusion_results_2016_05_31_11h22m59s.input_solar_cost_multipliers b
	ON a.sector_abbr = b.sector_abbr
LEFT JOIN diffusion_results_2016_05_31_11h22m59s.input_solar_performance_improvements c
	ON a.year = c.year


CROSS JOIN diffusion_results_2016_05_31_11h22m59s.input_main_scenario_options d
LEFT JOIN diffusion_shared.aeo_load_growth_projections e
	ON a.year = e.year
	AND a.sector_abbr = e.sector_abbr
	AND lower(e.scenario) = lower(d.load_growth_scenario)
LEFT JOIN diffusion_results_2016_05_31_11h22m59s.input_main_market_projections f
	ON a.year = f.year
LEFT JOIN diffusion_shared.state_census_division_abbr_lkup g
	ON e.census_division_abbr = g.census_division_abbr
LEFT JOIN diffusion_results_2016_05_31_11h22m59s.carbon_intensities_to_model h
	ON g.state_abbr = h.state_abbr
	AND a.year = h.year
LEFT JOIN diffusion_results_2016_05_31_11h22m59s.input_solar_leasing_availability i
	ON g.state_abbr = i.state_abbr
	AND a.year = i.year            
where g.state_abbr not in ('AK', 'HI');   
-- row count should be 49 states * 19 years * 3 sectors   
-- 2793
                        
                
