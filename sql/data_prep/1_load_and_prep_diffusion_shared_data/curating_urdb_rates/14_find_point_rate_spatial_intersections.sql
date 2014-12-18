-- create lookup tables that give the rates that intersect each point
-- in the res/com/ind point tables

-- intersect curated rates against commercial point grid to get a lookup table
DROP TABLE IF EXISTS diffusion_shared.curated_urdb_rates_lookup_pts_com;
CREATE TABLE diffusion_shared.curated_urdb_rates_lookup_pts_com
(
	pt_gid integer,
	state_abbr character varying(2),
	hdf_load_index integer,
	census_division_abbr text,
	urdb_rate_id text,
	urdb_utility_type text,
	urdb_rate_type text,
	urdb_demand_min numeric,
	urdb_demand_max numeric
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_com','gid',
		'SELECT a.gid as pt_gid, 
			c.state_abbr,
			a.hdf_load_index,
			c.census_division_abbr,
			b.urdb_rate_id, 
			b.utility_type as urdb_utility_type,
			b.rate_type as urdb_rate_type,
			b.demand_min as urdb_demand_min,
			b.demand_max as urdb_demand_max
		FROM diffusion_shared.pt_grid_us_com a
		INNER JOIN diffusion_shared.curated_urdb_rates_com b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		INNER JOIN diffusion_shared.county_geom c
		ON a.county_id = c.county_id;',
		'diffusion_shared.curated_urdb_rates_lookup_pts_com', 'a',16);

-- add indices
CREATE INDEX curated_urdb_rates_lookup_pts_com_pt_gid_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_com
using btree(pt_gid);

CREATE INDEX curated_urdb_rates_lookup_pts_com_state_abbr_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_com
using btree(state_abbr);

CREATE INDEX curated_urdb_rates_lookup_pts_com_hdf_load_index_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_com
using btree(hdf_load_index);

CREATE INDEX curated_urdb_rates_lookup_pts_com_census_division_abbr_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_com
using btree(census_division_abbr);

CREATE INDEX curated_urdb_rates_lookup_pts_com_urdb_rate_id_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_com
using btree(urdb_rate_id);

CREATE INDEX curated_urdb_rates_lookup_pts_com_urdb_utility_type_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_com
using btree(urdb_utility_type);


-- intersect against industrial point grid to get a lookup table
DROP TABLE IF EXISTS diffusion_shared.curated_urdb_rates_lookup_pts_ind;
CREATE TABLE diffusion_shared.curated_urdb_rates_lookup_pts_ind
(
	pt_gid integer,
	state_abbr character varying(2),
	hdf_load_index integer,
	census_division_abbr text,
	urdb_rate_id text,
	urdb_utility_type text,
	urdb_rate_type text,
	urdb_demand_min numeric,
	urdb_demand_max numeric
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_ind','gid',
		'SELECT a.gid as pt_gid, 
			c.state_abbr,
			a.hdf_load_index,
			c.census_division_abbr,
			b.urdb_rate_id, 
			b.utility_type as urdb_utility_type,
			b.rate_type as urdb_rate_type,
			b.demand_min as urdb_demand_min,
			b.demand_max as urdb_demand_max
		FROM diffusion_shared.pt_grid_us_ind a
		INNER JOIN diffusion_shared.curated_urdb_rates_com b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		INNER JOIN diffusion_shared.county_geom c
		ON a.county_id = c.county_id;',
		'diffusion_shared.curated_urdb_rates_lookup_pts_ind', 'a', 16);

-- create indices
CREATE INDEX curated_urdb_rates_lookup_pts_ind_pt_gid_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_ind
using btree(pt_gid);

CREATE INDEX curated_urdb_rates_lookup_pts_ind_state_abbr_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_ind
using btree(state_abbr);

CREATE INDEX curated_urdb_rates_lookup_pts_ind_hdf_load_index_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_ind
using btree(hdf_load_index);

CREATE INDEX curated_urdb_rates_lookup_pts_ind_census_division_abbr_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_ind
using btree(census_division_abbr);

CREATE INDEX curated_urdb_rates_lookup_pts_ind_urdb_rate_id_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_ind
using btree(urdb_rate_id);

CREATE INDEX curated_urdb_rates_lookup_pts_ind_urdb_utility_type_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_ind
using btree(urdb_utility_type);

--------------------------------------------------------------------------------
-- intersect against residential point grid to get a lookup table
DROP TABLE IF EXISTS diffusion_shared.curated_urdb_rates_lookup_pts_res;
CREATE TABLE diffusion_shared.curated_urdb_rates_lookup_pts_res
(
	pt_gid integer,
	state_abbr character varying(2),
	hdf_load_index integer,
	recs_2009_reportable_domain text,
	urdb_rate_id text,
	urdb_utility_type text,
	urdb_rate_type text,
	urdb_demand_min numeric,
	urdb_demand_max numeric
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_res','gid',
		'SELECT a.gid as pt_gid, 
			c.state_abbr,
			a.hdf_load_index,
			c.recs_2009_reportable_domain,
			b.urdb_rate_id, 
			b.utility_type as urdb_utility_type,
			b.rate_type as urdb_rate_type,
			b.demand_min as urdb_demand_min,
			b.demand_max as urdb_demand_max
		FROM diffusion_shared.pt_grid_us_res a
		INNER JOIN diffusion_shared.curated_urdb_rates_res b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		INNER JOIN diffusion_shared.county_geom c
		ON a.county_id = c.county_id;',
		'diffusion_shared.curated_urdb_rates_lookup_pts_res', 'a',16);

-- add indices
CREATE INDEX curated_urdb_rates_lookup_pts_res_pt_gid_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_res
using btree(pt_gid);

CREATE INDEX curated_urdb_rates_lookup_pts_res_state_abbr_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_res
using btree(state_abbr);

CREATE INDEX curated_urdb_rates_lookup_pts_res_hdf_load_index_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_res
using btree(hdf_load_index);

CREATE INDEX curated_urdb_rates_lookup_pts_res_recs_2009_reportable_domain_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_res
using btree(recs_2009_reportable_domain);

CREATE INDEX curated_urdb_rates_lookup_pts_res_urdb_rate_id_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_res
using btree(urdb_rate_id);

CREATE INDEX curated_urdb_rates_lookup_pts_res_urdb_utility_type_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_res
using btree(urdb_utility_type);
