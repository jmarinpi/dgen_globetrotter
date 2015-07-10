select *
from diffusion_template.input_main_scenario_options;


select *
FROM diffusion_template.input_main_market_inflation;


select *
FROM diffusion_template.input_main_market_projections
order by 1;

-- select *
-- FROM diffusion_solar.market_projections
-- order by 1;


select *
from diffusion_template.input_main_market_flat_electric_rates
order by 1;

-- select *
-- from diffusion_solar.user_defined_electric_rates
-- order by 1;


select *
from diffusion_template.input_main_market_rate_type_weights
order by 1;

-- select *
-- from diffusion_solar.rate_type_weights
-- order by 1;


select *
FROM diffusion_template.input_main_market_carbon_intensities
order by 1;

-- select *
-- FROM diffusion_solar.manual_carbon_intensities
-- order by 1;


select *
FROM diffusion_template.input_main_nem_utility_types
order by 1;


select *
FROM diffusion_template.input_main_nem_avoided_costs
order by 1;

-- select *
-- FROM diffusion_solar.nem_scenario_avoided_costs
-- order by 1


select *
FROM  diffusion_template.input_main_nem_selected_scenario;


select *
FROM diffusion_template.input_main_nem_user_defined_scenario;


select *
FROM diffusion_template.input_main_nem_scenario;

-- select *
-- FROM diffusion_shared.nem_scenario_bau;
-- 
-- select *
-- FROM diffusion_shared.nem_scenario_none_everywhere;
-- 
-- select *
-- FROM diffusion_shared.nem_scenario_full_everywhere;


select *
FROM diffusion_template.input_solar_cost_projections_res
order by 1;

select *
FROM diffusion_template.input_solar_cost_projections_com
order by 1;

select *
FROM diffusion_template.input_solar_cost_projections_ind
order by 1;

select *
FROM diffusion_template.input_solar_cost_projections
order by 1;

-- select *
-- FROM diffusion_solar.solar_cost_projections
-- order by 1;


select *
FROM diffusion_template.input_solar_cost_learning_rates
order by 1;

-- select *
-- FROM diffusion_solar.learning_rates
-- order by 1;


select *
FROM diffusion_template.input_solar_cost_assumptions
order by 1;

-- select cost_assumptions
-- from diffusion_solar.scenario_options;


select *
FROM diffusion_template.input_solar_cost_projections_to_model
order by 1, sector;

-- select *
-- from diffusion_solar.cost_projections_to_model
-- order by 1, sector;


select *
FROM diffusion_template.input_solar_performance_improvements
order by 1;

-- select *
-- FROM diffusion_solar.solar_performance_improvements
-- order by 1;


select *
from diffusion_template.input_solar_performance_annual_system_degradation;

-- select ann_system_degradation
-- from diffusion_solar.scenario_options;


select *
FROM diffusion_template.input_solar_performance_system_sizing_factors
order by 1;

-- select *
-- from diffusion_solar.system_sizing_factors
-- order by 1;



