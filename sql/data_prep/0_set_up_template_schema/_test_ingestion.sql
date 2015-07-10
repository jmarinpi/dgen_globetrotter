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


