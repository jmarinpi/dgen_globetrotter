set role 'diffusion-writers';

DROP VIEW IF EXISTS diffusion_results_2016_05_31_11h22m59s.temporal_factors_general;
CREATE VIEW diffusion_results_2016_05_31_11h22m59s.temporal_factors_general AS
SELECT 	a.year,
	d.state_abbr,
	c.census_division_abbr,
	-- load growth
	c.scenario as load_growth_scenario,
	c.load_multiplier,
	-- carbon tax 
	a.carbon_dollars_per_ton,
	e.t_co2_per_kwh,
	e.t_co2_per_kwh * 100 * a.carbon_dollars_per_ton as carbon_price_cents_per_kwh
FROM diffusion_results_2016_05_31_11h22m59s.input_main_market_projections a
CROSS JOIN diffusion_results_2016_05_31_11h22m59s.input_main_scenario_options b
LEFT JOIN diffusion_shared.aeo_load_growth_projections c
	ON a.year = c.year
	AND lower(c.scenario) = lower(b.load_growth_scenario)
LEFT JOIN diffusion_shared.state_census_division_abbr_lkup d
	ON c.census_division_abbr = d.census_division_abbr
LEFT JOIN diffusion_results_2016_05_31_11h22m59s.carbon_intensities_to_model e
	ON d.state_abbr = e.state_abbr
	AND a.year = e.year
where d.state_abbr not in ('AK', 'HI');   
-- row count should be 49 states * 19 years * 3 sectors   
-- 2793
                        
                
