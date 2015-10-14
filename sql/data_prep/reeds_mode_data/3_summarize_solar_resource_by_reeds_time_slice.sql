set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice;
CREATE TABLE diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice 
(
  solar_re_9809_gid integer,
  tilt integer,
  azimuth character varying(2),
  reeds_time_slice character varying(3),
  cf_avg numeric
);



-- east
select parsel_2('dav-gis','mgleason', 'mgleason',
		'diffusion_solar.solar_resource_hourly_e','solar_re_9809_gid',
		'
		with b as
		(
			select reeds_time_slice, array_agg(hour_index) as hour_indices
			from diffusion_solar.reeds_time_slice_lkup a
			GROUP BY reeds_time_slice
		)
		select a.solar_re_9809_gid, a.tilt, a.azimuth, b.reeds_time_slice, 
			round(r_mean_array(a.cf, b.hour_indices)/1e6, 6) as cf_avg
		from diffusion_solar.solar_resource_hourly_e a
		CROSS JOIN b;

		',
		'diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice', 
		'a', 16);

-- west
select parsel_2('dav-gis','mgleason', 'mgleason',
		'diffusion_solar.solar_resource_hourly_w','solar_re_9809_gid',
		'
		with b as
		(
			select reeds_time_slice, array_agg(hour_index) as hour_indices
			from diffusion_solar.reeds_time_slice_lkup a
			GROUP BY reeds_time_slice
		)
		select a.solar_re_9809_gid, a.tilt, a.azimuth, b.reeds_time_slice, 
			round(r_mean_array(a.cf, b.hour_indices)/1e6, 6) as cf_avg
		from diffusion_solar.solar_resource_hourly_w a
		CROSS JOIN b;

		',
		'diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice', 
		'a', 24);


-- se
select parsel_2('dav-gis','mgleason', 'mgleason',
		'diffusion_solar.solar_resource_hourly_se','solar_re_9809_gid',
		'
		with b as
		(
			select reeds_time_slice, array_agg(hour_index) as hour_indices
			from diffusion_solar.reeds_time_slice_lkup a
			GROUP BY reeds_time_slice
		)
		select a.solar_re_9809_gid, a.tilt, a.azimuth, b.reeds_time_slice, 
			round(r_mean_array(a.cf, b.hour_indices)/1e6, 6) as cf_avg
		from diffusion_solar.solar_resource_hourly_se a
		CROSS JOIN b;

		',
		'diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice', 
		'a', 24);
-- ** working on above right now
-- sw
select parsel_2('dav-gis','mgleason', 'mgleason',
		'diffusion_solar.solar_resource_hourly_sw','solar_re_9809_gid',
		'
		with b as
		(
			select reeds_time_slice, array_agg(hour_index) as hour_indices
			from diffusion_solar.reeds_time_slice_lkup a
			GROUP BY reeds_time_slice
		)
		select a.solar_re_9809_gid, a.tilt, a.azimuth, b.reeds_time_slice, 
			round(r_mean_array(a.cf, b.hour_indices)/1e6, 6) as cf_avg
		from diffusion_solar.solar_resource_hourly_sw a
		CROSS JOIN b;

		',
		'diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice', 
		'a', 24);


-- south
select parsel_2('dav-gis','mgleason', 'mgleason',
		'diffusion_solar.solar_resource_hourly_s','solar_re_9809_gid',
		'
		with b as
		(
			select reeds_time_slice, array_agg(hour_index) as hour_indices
			from diffusion_solar.reeds_time_slice_lkup a
			GROUP BY reeds_time_slice
		)
		select a.solar_re_9809_gid, a.tilt, a.azimuth, b.reeds_time_slice, 
			round(r_mean_array(a.cf, b.hour_indices)/1e6, 6) as cf_avg
		from diffusion_solar.solar_resource_hourly_s a
		CROSS JOIN b;

		',
		'diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice', 
		'a', 24);


-- create indices
CREATE INDEX reeds_avg_cf_by_orientation_and_time_slice_gid_btree
ON diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice
USING BTREE(solar_re_9809_gid);

CREATE INDEX reeds_avg_cf_by_orientation_and_time_slice_tilt_azi_time_btree
ON diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice
USING BTREE(tilt, azimuth, reeds_time_slice);

vACUUM ANALYZE diffusion_solar.reeds_avg_cf_by_orientation_and_time_slice;