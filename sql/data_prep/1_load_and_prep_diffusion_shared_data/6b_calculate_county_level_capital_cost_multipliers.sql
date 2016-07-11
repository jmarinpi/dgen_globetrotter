-- Regional Capital Cost Multipliers by County

-- export county geoms to shapefile
pgsql2shp -g the_geom_96703_20m -f /Volumes/Staff/mgleason/dGeo/Data/Analysis/reg_cap_cost_zonal_stats_bycnty/cnty/cnty_20m.shp -h gispgdb -u mmooney -P mmooney dav-gis "select state_fips, county_fips, geoid10, the_geom_96703_20m from diffusion_blocks.county_geoms"

-- import dual and binary csvs into temporary tables
drop table if exists diffusion_geo.temp_gtdual;
create table diffusion_geo.temp_gtdual 
	(
	objectid numeric,
	geoid character varying(5),
	zone_code numeric,
	count numeric,
	area numeric,
	mean numeric
	);
drop table if exists diffusion_geo.temp_gtbinary;
create table diffusion_geo.temp_gtbinary
	(
	objectid numeric,
	geoid character varying(5),
	zone_code numeric,
	count numeric,
	area numeric,
	mean numeric
	);

\COPY diffusion_geo.temp_gtbinary FROM '/Volumes/Staff/mgleason/dGeo/Data/Analysis/reg_cap_cost_zonal_stats_bycnty/avg_gitbinary_per_cnty.csv' with csv header;
\COPY diffusion_geo.temp_gtdual FROM '/Volumes/Staff/mgleason/dGeo/Data/Analysis/reg_cap_cost_zonal_stats_bycnty/avg_gitdual_per_cnty.csv' with csv header;

-- Create final table
drop table if exists diffusion_geo.regional_cap_cost_multipliers;
create table diffusion_geo.regional_cap_cost_multipliers  as
(
	select a.geoid10 as county_id,
	b.mean as cap_cost_multiplier_gt_binary,
	c.mean as cap_cost_multiplier_gt_dual
	from diffusion_blocks.county_geoms a
	left join diffusion_geo.temp_gtbinary b
	on a.geoid10 = b.geoid
	left join diffusion_geo.temp_gtdual c
	on a.geoid10 = c.geoid);

)
-- Add primary key
alter table diffusion_geo.regional_cap_cost_multipliers
add constraint regional_cap_cost_multipliers_county_county_id_pkey
primary key (county_id);

-- delete temp tables
drop table if exists diffusion_geo.temp_gtdual;
drop table if exists diffusion_geo.temp_gtbinary;


-- Transform Null Values into "Nearest Neighbor"


-- *** TODO!!!
-- Null Values-- should I update this given a value of 0 really means null?
-- Update null values to 0
update diffusion_geo.regional_cap_cost_multipliers
set cap_cost_multiplier_gt_binary = 


