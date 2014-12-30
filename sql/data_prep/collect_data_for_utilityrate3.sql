-- eplus scale offset = 1e8
-- solar generation scale offset = 1e6
-- wind generation scale offset = 1e3
with eplus as 
(
	SELECT hdf_index, crb_model, nkwh
	FROM diffusion_shared.energy_plus_normalized_load_res
	where crb_model = 'reference'
	UNION ALL
	SELECT hdf_index, crb_model, nkwh
	FROM diffusion_shared.energy_plus_normalized_load_com
)


select 	a.uid, 

	b.sam_json, 

	a.load_kwh_per_customer_in_bin as annual_load,
	c.nkwh as norm_hrly_load,
	
	a.system_size_kw,
	d.cf as hourly_cf
	
FROM diffusion_solar.unique_rate_gen_load_combinations a
-- join the rate data
LEFT JOIN diffusion_shared.urdb3_rate_sam_jsons b 
ON a.rate_id_alias = b.rate_id_alias
-- join the load data
LEFT JOIN eplus c
ON a.crb_model = c.crb_model
AND a.hdf_load_index = c.hdf_index
-- join the resource data
LEFT JOIN diffusion_solar.solar_resource_hourly d
ON a.solar_re_9809_gid = d.solar_re_9809_gid
and a.tilt = d.tilt
and a.azimuth = d.azimuth
limit 10;


with eplus as 
(
	SELECT hdf_index, crb_model, nkwh
	FROM diffusion_shared.energy_plus_normalized_load_res
	where crb_model = 'reference'
	UNION ALL
	SELECT hdf_index, crb_model, nkwh
	FROM diffusion_shared.energy_plus_normalized_load_com
)


select 	a.uid, 

	b.sam_json, 

	a.load_kwh_per_customer_in_bin as annual_load,
	c.nkwh as norm_hrly_load,
	
	a.system_size_kw,
	d.cf as hourly_cf
	
FROM diffusion_solar.unique_rate_gen_load_combinations a
-- join the rate data
LEFT JOIN diffusion_shared.urdb3_rate_sam_jsons b 
ON a.rate_id_alias = b.rate_id_alias
-- join the load data
LEFT JOIN eplus c
ON a.crb_model = c.crb_model
AND a.hdf_load_index = c.hdf_index
-- join the resource data
LEFT JOIN diffusion_wind.wind_resource_hourly d
ON a.i = d.i
and a.j = d.j
and a.cf_bin = d.cf_bin
and a.turbine_height_m = d.height
and a.turbine_id = d.turbine_id
limit 10;
