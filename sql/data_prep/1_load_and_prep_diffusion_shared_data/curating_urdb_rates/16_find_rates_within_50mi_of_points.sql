-- find all rates within 50 mi of each point with the same utility_type

-- COMMERCIAL
DROP TABLE IF EXISTS diffusion_shared.ranked_urdb_rates_by_pt_com;
CREATE TABLE diffusion_shared.ranked_urdb_rates_by_pt_com
(
	pt_gid integer,
	urdb_rate_id text,
	rank integer,
	intersects_rate boolean,
	near_utility_type_match boolean
);



SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_com','gid',
		'WITH a as
			(
				SELECT a.gid as pt_gid, d.urdb_rate_id, 
					(a.utility_type = d.utility_type) as utility_type_match,
					ST_Distance(a.the_geom_900914, d.the_geom_900914) as distance_m
				FROM diffusion_shared.pt_grid_us_com a
				INNER JOIN diffusion_shared.county_geom b
				ON a.county_id = b.county_id
				INNER JOIN diffusion_shared.urdb_rates_by_state_com c
				ON b.state_abbr = c.state_abbr
				INNER JOIN diffusion_shared.curated_urdb_rates_com d
				ON c.urdb_rate_id = d.urdb_rate_id
			),
			b as
			(
				select pt_gid, urdb_rate_id, utility_type_match, min(distance_m) as distance_m
				FROM a
				GROUP BY pt_gid, urdb_rate_id, utility_type_match
			),
			c as
			(
				SELECT pt_gid, urdb_rate_id, 
					(utility_type_match = true and distance_m <= 80467.2)::integer as near_utility_type_match,
					distance_m
				from b
			)
				SELECT pt_gid,  urdb_rate_id, rank() OVER (partition by pt_gid ORDER BY near_utility_type_match desc, distance_m asC) as rank,
					distance_m = 0 as intersects_rate,
					near_utility_type_match::boolean
				FROM c;',
		'diffusion_shared.ranked_urdb_rates_by_pt_com', 'a', 18);

-- 198913815 rows
-- add indices
CREATE INDEX ranked_urdb_rates_by_pt_com_pt_gid_btree
ON diffusion_shared.ranked_urdb_rates_by_pt_com
using btree(pt_gid);

CREATE INDEX ranked_urdb_rates_by_pt_com_urdb_rate_id_btree
ON diffusion_shared.ranked_urdb_rates_by_pt_com
using btree(urdb_rate_id);

CREATE INDEX ranked_urdb_rates_by_pt_com_urdb_rank_btree
ON diffusion_shared.ranked_urdb_rates_by_pt_com
using btree(rank);