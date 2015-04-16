﻿SET ROLE 'diffusion-writers';
DROP VIEW IF EXISTS diffusion_shared.energy_plus_normalized_demand;

CREATE OR REPLACE VIEW diffusion_shared.energy_plus_normalized_demand
AS 
SELECT hdf_index, crb_model, normalized_max_demand_kw_per_kw, annual_sum_kwh
FROM diffusion_shared.energy_plus_max_normalized_demand_com
UNION
SELECT hdf_index, crb_model, normalized_max_demand_kw_per_kw, annual_sum_kwh
FROM diffusion_shared.energy_plus_max_normalized_demand_res
;

COMMENT ON VIEW diffusion_shared.energy_plus_normalized_demand IS 'Combines energy_plus_max_normalized_demand_com and energy_plus_max_normalized_demand_res tables';