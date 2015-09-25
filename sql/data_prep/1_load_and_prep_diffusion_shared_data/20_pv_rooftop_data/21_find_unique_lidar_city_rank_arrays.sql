-- find the unique combinations of ranked rates (some points will have identical rankings)
------------------------------------------------------------------------------------------------------------
-- COMMERCIAL
------------------------------------------------------------------------------------------------------------
-- group the ranked urdb rates into arrays for each individual point 
DROP TABLE IF EXISTS diffusion_data_shared.pt_ranked_lidar_city_arrays_com;
CREATE TABLE diffusion_data_shared.pt_ranked_lidar_city_arrays_com
(
	pt_gid integer,
	city_id_array integer[],
	rank_array integer[]
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_ranked_lidar_city_lkup_res','pt_gid',
		'SELECT a.pt_gid, 
			array_agg(a.city_id order by a.rank asc, a.city_id asc) as city_id_array,
			array_agg(a.rank order by a.rank asc, a.city_id asc) as rank_array
		FROM diffusion_shared.pt_ranked_lidar_city_lkup_res a
		GROUP BY pt_gid;',
		'diffusion_data_shared.pt_ranked_lidar_city_arrays_com', 'a', 16);

-- check that the row count matches the row count of diffusion_shared.pt_grid_us_com
SELECT count(*)
FROM diffusion_shared.pt_grid_us_com;
-- 

select count(*)
FROM diffusion_data_shared.pt_ranked_lidar_city_arrays_com;
-- 

-- make sure there are no emtpy arrays
select count(*)
FROM diffusion_data_shared.pt_ranked_lidar_city_arrays_com
where city_id_array is null;
-- 0
select count(*)
FROM diffusion_data_shared.pt_ranked_lidar_city_arrays_com
where city_id_array = '{}';
-- 0
-- all set

-- create indices
CREATE INDEX pt_ranked_lidar_city_arrays_com_city_id_btree 
ON diffusion_data_shared.pt_ranked_lidar_city_arrays_com
using btree(city_id_array);

CREATE INDEX pt_ranked_lidar_city_arrays_com_rank_btree 
ON diffusion_data_shared.pt_ranked_lidar_city_arrays_com
using btree(rank_array);

CREATE INDEX pt_ranked_lidar_city_arrays_com_gid_btree 
ON diffusion_data_shared.pt_ranked_lidar_city_arrays_com
using btree(pt_gid);

------------------------------------------------------------------------------------------------------------
-- find the distinct ranked rate arrays across all points
DROP TABLE IF EXISTS diffusion_data_shared.unique_ranked_lidar_city_arrays_com;
CREATE TABLE diffusion_data_shared.unique_ranked_lidar_city_arrays_com AS
SELECT city_id_array, rank_array
FROM diffusion_data_shared.pt_ranked_lidar_city_arrays_com
GROUP BY city_id_array, rank_array;
-- ? rows

-- add a unique id for each unique ranked rate array
ALTER TABLE diffusion_data_shared.unique_ranked_lidar_city_arrays_com
ADD ranked_city_array_id serial primary key;

-- create index on the arrays
CREATE INDEX unique_ranked_lidar_city_arrays_com_rate_id_btree
ON diffusion_data_shared.unique_ranked_lidar_city_arrays_com 
USING btree(city_id_array);

CREATE INDEX unique_ranked_lidar_city_arrays_com_rank_btree
ON diffusion_data_shared.unique_ranked_lidar_city_arrays_com 
USING btree(rank_array);


------------------------------------------------------------------------------------------------------------
-- create lookup table for each point to a ranked_city_array_id
DROP TABLE IF EXISTS diffusion_data_shared.pt_ranked_lidar_city_array_lkup_com;
CREATE TABLE diffusion_data_shared.pt_ranked_lidar_city_array_lkup_com
(
	pt_gid integer,
	ranked_city_array_id integer
);


SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_data_shared.pt_ranked_lidar_city_arrays_com','pt_gid',
		'SELECT a.pt_gid, b.ranked_city_array_id
		FROM diffusion_data_shared.pt_ranked_lidar_city_arrays_com a
		LEFT JOIN diffusion_data_shared.unique_ranked_lidar_city_arrays_com b
		ON a.city_id_array = b.city_id_array
		and a.rank_array = b.rank_array;',
		'diffusion_data_shared.pt_ranked_lidar_city_array_lkup_com', 'a', 16);

select count(*)
FROM diffusion_data_shared.pt_ranked_lidar_city_array_lkup_com;
-- ? rows

select count(*)
FROM diffusion_shared.pt_grid_us_com;
-- matches pt grid com (?)

-- add primary key
ALTER TABLE diffusion_data_shared.pt_ranked_lidar_city_array_lkup_com
ADD PRIMARY KEY (pt_gid);

-- add index
CREATE INDEX pt_ranked_lidar_city_array_lkup_com_rate_array_btree
ON diffusion_data_shared.pt_ranked_lidar_city_array_lkup_com
using btree(ranked_city_array_id);

-- make sure no nulls
SELECT count(*)
FROM diffusion_data_shared.pt_ranked_lidar_city_array_lkup_com
where ranked_city_array_id is null;

------------------------------------------------------------------------------------------------------------
-- add the ranked_city_array_id back to the main pt table
ALTER TABLE diffusion_shared.pt_grid_us_com
ADD COLUMN ranked_city_array_id integer;

UPDATE diffusion_shared.pt_grid_us_com a
set ranked_city_array_id = b.ranked_city_array_id
from diffusion_data_shared.pt_ranked_lidar_city_array_lkup_com b
where a.gid = b.pt_gid;
-- ** make sure to update microdata and pt join view to account for ranked_city_array_id now **

-- add an index
CREATE INDEX pt_grid_us_com_ranked_city_array_id_btree
ON diffusion_shared.pt_grid_us_com
USING btree(ranked_city_array_id);

-- check no nulls
SELECT count(*)
FROM diffusion_shared.pt_grid_us_com
where ranked_city_array_id is null;

------------------------------------------------------------------------------------------------------------
-- unnest the unique ranked rate arrays into normal table structure
DROP TABLE IF EXISTS diffusion_shared.ranked_rate_array_lkup_com;
CREATE TABLE diffusion_shared.ranked_rate_array_lkup_com as
SELECT ranked_city_array_id, 
	unnest(city_id_array) as city_id, 
	unnest(rank_array) as rank
FROM diffusion_data_shared.unique_ranked_lidar_city_arrays_com;
-- ?

-- add indices
CREATE INDEX ranked_rate_array_lkup_com_ranked_city_array_id_btree
ON diffusion_shared.ranked_rate_array_lkup_com
using btree(ranked_city_array_id);

CREATE INDEX ranked_rate_array_lkup_com_city_id_btree
ON diffusion_shared.ranked_rate_array_lkup_com
using btree(city_id);

CREATE INDEX ranked_rate_array_lkup_com_rank_btree
ON diffusion_shared.ranked_rate_array_lkup_com
using btree(rank);
------------------------------------------------------------------------------------------------------------


-- 
-- -- do some testing:
-- with a AS
-- (
-- 	SELECT a.county_id, a.bin_id, 
-- 		a.ranked_city_array_id,
-- 		a.max_demand_kw, 
-- 		a.state_abbr,
-- 		b.rate_id_alias,
-- 		c.rank as rate_rank,
-- 		d.rate_type
-- 	FROM diffusion_solar.pt_com_best_option_each_year a
-- 	LEFT JOIN diffusion_shared.urdb_rates_by_state_com b
-- 	ON a.max_demand_kw <= b.urdb_demand_max
-- 	and a.max_demand_kw >= b.urdb_demand_min
-- 	and a.state_abbr = b.state_abbr
-- 	LEFT JOIN diffusion_shared.ranked_rate_array_lkup_com c
-- 	ON a.ranked_city_array_id = c.ranked_city_array_id
-- 	and b.rate_id_alias = c.rate_id_alias
-- 	LEFT JOIN urdb_rates.combined_singular_verified_rates_lookup d
-- 	on b.rate_id_alias = d.rate_id_alias
-- 	and d.res_com = 'C'
-- 	where a.year = 2014
-- ),
-- b as
-- (
-- 	SELECT *, row_number() OVER (partition by county_id, bin_id order by rate_rank asc) as rank
-- 		-- *** THIS SHOULD BE A RANK WITH A SUBSEQUENT TIE BREAKER BASED ON USER DEFINED RATE TYPE PROBABILITIES
-- 	FROM a
-- )
-- SELECT *
-- FROM b 
-- where rank = 1;
-- 
-- select distinct(rate_type)
-- FROM urdb_rates.combined_singular_verified_rates_lookup
-- order by 1;


