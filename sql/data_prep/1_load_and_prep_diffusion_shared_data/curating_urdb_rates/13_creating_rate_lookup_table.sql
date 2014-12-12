-- COMMERCIAL/INDUSTRIAL
-- create a simple table with each urdb_rate_id linked to a ventyx geom
DROP TABLE IF EXISTS diffusion_shared.curated_urdb_rates_com;
CREATE TABLE diffusion_shared.curated_urdb_rates_com AS
with a as
(
	-- collect all of the commercial rates
	SELECT urdb_rate_id, utility_name as ur_name, sub_territory_name
	FROM urdb_rates.urdb3_verified_rates_lookup_20141202
	where res_com = 'C'
	UNION ALl
	SELECT urdb_rate_id, utility_name as ur_name, sub_territory_name
	FROM urdb_rates.urdb3_singular_rates_lookup_20141202	
	where res_com = 'C'
	and verified = False

),
b as 
(
	SELECT a.*, b.ventyx_company_id_2014
	FROM a
	LEFT JOIN urdb_rates.urdb3_verified_and_singular_ur_names_20141202 b
	ON a.ur_name = b.ur_name
)
select b.*, c.the_geom_4326, c.gid as geom_gid, c.company_type_general as utility_type
from b
left join urdb_rates.ventyx_electric_service_territories_w_vs_rates_20141202 c
ON b.ventyx_company_id_2014 = c.company_id::text
and b.sub_territory_name = c.sub_territory_name;

-- make sure there are no nulls
select *
FROM diffusion_shared.curated_urdb_rates_com
where the_geom_4326 is null;

-- create primary key on the urdb_rate_id
ALTER TABLE diffusion_shared.curated_urdb_rates_com
ADD PRIMARY KEY (urdb_rate_id, geom_gid);

-- create index on the geometry
CREATE INDEX curated_urdb_rates_com_the_geom_4326_gist
ON  diffusion_shared.curated_urdb_rates_com
using gist(the_geom_4326);

-- intersect against commercial point grid to get a lookup table
DROP TABLE IF EXISTS diffusion_shared.curated_urdb_rates_lookup_pts_com;
CREATE TABLE diffusion_shared.curated_urdb_rates_lookup_pts_com
(
	pt_gid integer,
	hdf_load_index integer,
	census_division_abbr text,
	urdb_rate_id text,
	utility_type text
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_com','gid',
		'SELECT a.gid as pt_gid, 
			a.hdf_load_index,
			c.census_division_abbr,
			b.urdb_rate_id, b.utility_type
		FROM diffusion_shared.pt_grid_us_com a
		LEFT JOIN diffusion_shared.curated_urdb_rates_com b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		LEFT JOIN diffusion_shared.county_geom c
		ON a.county_id = c.county_id;',
		'diffusion_shared.curated_urdb_rates_lookup_pts_com', 'a',16);

-- add indices
CREATE INDEX curated_urdb_rates_lookup_pts_com_pt_gid_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_com
using btree(pt_gid);

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
using btree(utility_type);


-- intersect against industrial point grid to get a lookup table
DROP TABLE IF EXISTS diffusion_shared.curated_urdb_rates_lookup_pts_ind;
CREATE TABLE diffusion_shared.curated_urdb_rates_lookup_pts_ind
(
	pt_gid integer,
	hdf_load_index integer,
	census_division_abbr text,
	urdb_rate_id text,
	utility_type text
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_ind','gid',
		'SELECT a.gid as pt_gid, a.hdf_load_index,
			c.census_division_abbr,
			b.urdb_rate_id, b.utility_type
		FROM diffusion_shared.pt_grid_us_ind a
		LEFT JOIN diffusion_shared.curated_urdb_rates_com b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		LEFT JOIN diffusion_shared.county_geom c
		ON a.county_id = c.county_id;',
		'diffusion_shared.curated_urdb_rates_lookup_pts_ind', 'a', 16);

-- create indices
CREATE INDEX curated_urdb_rates_lookup_pts_ind_pt_gid_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_ind
using btree(pt_gid);

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
using btree(utility_type);

--------------------------------------------------------------------------------

-- RESIDENTIAL
-- create a simple table with each urdb_rate_id linked to a ventyx geom
DROP TABLE IF EXISTS diffusion_shared.curated_urdb_rates_res;
CREATE TABLE diffusion_shared.curated_urdb_rates_res AS
with a as
(
	-- collect all of the residential rates
	SELECT urdb_rate_id, utility_name as ur_name, sub_territory_name
	FROM urdb_rates.urdb3_verified_rates_lookup_20141202
	where res_com = 'R'
	UNION ALl
	SELECT urdb_rate_id, utility_name as ur_name, sub_territory_name
	FROM urdb_rates.urdb3_singular_rates_lookup_20141202	
	where res_com = 'R'
	and verified = False

),
b as 
(
	SELECT a.*, b.ventyx_company_id_2014
	FROM a
	LEFT JOIN urdb_rates.urdb3_verified_and_singular_ur_names_20141202 b
	ON a.ur_name = b.ur_name
)
select b.*, c.the_geom_4326, c.gid as geom_gid, c.company_type_general as utility_type
from b
left join urdb_rates.ventyx_electric_service_territories_w_vs_rates_20141202 c
ON b.ventyx_company_id_2014 = c.company_id::text
and b.sub_territory_name = c.sub_territory_name;

-- make sure there are no nulls
select *
FROM diffusion_shared.curated_urdb_rates_res
where the_geom_4326 is null;

-- create primary key on the urdb_rate_id
ALTER TABLE diffusion_shared.curated_urdb_rates_res
ADD PRIMARY KEY (urdb_rate_id, geom_gid);

-- create index on the geometry
CREATE INDEX curated_urdb_rates_res_the_geom_4326_gist
ON  diffusion_shared.curated_urdb_rates_res
using gist(the_geom_4326);

-- intersect against commercial point grid to get a lookup table
DROP TABLE IF EXISTS diffusion_shared.curated_urdb_rates_lookup_pts_res;
CREATE TABLE diffusion_shared.curated_urdb_rates_lookup_pts_res
(
	pt_gid integer,
	hdf_load_index integer,
	recs_2009_reportable_domain text,
	urdb_rate_id text,
	utility_type text
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_res','gid',
		'SELECT a.gid as pt_gid, a.hdf_load_index,
			c.recs_2009_reportable_domain,
			b.urdb_rate_id, b.utility_type
		FROM diffusion_shared.pt_grid_us_res a
		LEFT JOIN diffusion_shared.curated_urdb_rates_res b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		LEFT JOIN diffusion_shared.county_geom c
		ON a.county_id = c.county_id;',
		'diffusion_shared.curated_urdb_rates_lookup_pts_res', 'a',16);

-- add indices
CREATE INDEX curated_urdb_rates_lookup_pts_res_pt_gid_btree
ON diffusion_shared.curated_urdb_rates_lookup_pts_res
using btree(pt_gid);

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
using btree(utility_type);


------------------------------------------------------------------------
-- find all multiplicative combinations by state: 
-- rates (assume each rate could apply anywhere in the state)
-- hdf indices
-- eia regions
with a as
(
	SELECT state_abbr, census_division_abbr
	from diffusion_shared.county_geom
	where state_abbr not in ('HI','AK')
	group by state_abbr, census_division_abbr
),
b as 
(
	SELECT b.state_abbr, a.hdf_load_index
	from diffusion_shared.pt_grid_us_com a
	left join diffusion_shared.county_geom b
	on a.county_id = b.county_id
	group by b.state_abbr, a.hdf_load_index
),
c as
(
	select c.state_abbr, a.urdb_rate_id, a.utility_type
	FROM diffusion_shared.curated_urdb_rates_lookup_pts_com a
	LEFT JOIN diffusion_shared.pt_grid_us_com b
	ON a.pt_gid = b.gid
	left join diffusion_shared.county_geom c
	on b.county_id = c.county_id 
	where a.urdb_rate_id is not null
	group by c.state_abbr, a.urdb_rate_id, a.utility_type
)
SELECT a.state_abbr, a.census_division_abbr, 
	b.hdf_load_index,
	c.urdb_rate_id, c.utility_type,
	d.demand_min, d.demand_max,
	d.rate_type
FROM a
left join b
on a.state_abbr = b.state_abbr
LEFT JOIN c
on b.state_abbr = c.state_abbr
LEFT JOIN urdb_rates.combined_singular_verified_rates_lookup d
ON c.urdb_rate_id = d.urdb_rate_id
and d.res_com = 'C';


select a.hdf_load_index, b.census_division_abbr
from diffusion_shared.pt_grid_us_com a
left join diffusion_shared.county_geom b
on a.county_id = b.county_id 
group by a.hdf_load_index, b.census_division_abbr
