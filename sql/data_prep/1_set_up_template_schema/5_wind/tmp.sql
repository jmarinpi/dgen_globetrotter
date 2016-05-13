
WITH turbine_sizes as
(
	select a.turbine_size_kw, a.rotor_radius_m,
		b.turbine_height_m,
		b.turbine_height_m - a.rotor_radius_m * c.canopy_clearance_rotor_factor as effective_min_blade_height_m,
		b.turbine_height_m + a.rotor_radius_m as effective_max_blade_height_m
	from diffusion_wind.turbine_size_to_rotor_radius_lkup a
	LEFT JOIN diffusion_results_2016_05_13_14h10m34s.input_wind_performance_allowable_turbine_sizes b
		ON a.turbine_size_kw = b.turbine_size_kw
	CROSS JOIN diffusion_results_2016_05_13_14h10m34s.input_wind_siting_settings_all c
),
points AS
(
	select a.*, b.*,
	      CASE WHEN a.canopy_pct >= b.canopy_pct_requiring_clearance * 100 THEN a.canopy_ht_m + b.canopy_clearance_static_adder_m
		   ELSE 0
	      END as min_allowable_blade_height_m,
	      CASE WHEN a.acres_per_bldg <= b.required_parcel_size_cap_acres THEN sqrt(a.acres_per_bldg * 4046.86)/(2 * b.blade_height_setback_factor)
		   ELSE 'Infinity'::double precision
	      end as max_allowable_blade_height_m
	FROM diffusion_results_2016_05_13_14h10m34s.pt_ind_sample_load_selected_rate_0 a
	CROSS JOIN diffusion_results_2016_05_13_14h10m34s.input_wind_siting_settings_all b
)
select a.*, 
	COALESCE(b.turbine_height_m, 0) AS turbine_height_m,
	COALESCE(b.turbine_size_kw, 0) as turbine_size_kw 
from points a
LEFT JOIN turbine_sizes b
	ON b.effective_min_blade_height_m >= a.min_allowable_blade_height_m 
	AND b.effective_max_blade_height_m <= a.max_allowable_blade_height_m;

