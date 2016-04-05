-- diffusion_shared.urdb_rates_by_state_res
-- diffusion_shared.urdb_rates_by_state_com
-- diffusion_shared.urdb_rates_by_state_ind


set role 'diffusion-writers';

-----------------------------------------------------------------------------------------------------------
-- commercial 
DROP TABLE IF EXISTS diffusion_blocks.block_urdb_rates_com;
CReATE TABLE diffusion_blocks.block_urdb_rates_com 
(
	pgid bigint,
	ranked_rate_ids integer[]
);

select parsel_2('dav-gis', 'mgleason', 'mgleason',
		'diffusion_blocks.block_geoms', 'pgid',
		'WITH a as
		(
			SELECT a.pgid, 
				c.rate_id_alias, 
				ST_Distance(a.the_point_96703, c.the_geom_96703) as distance_m
			FROM diffusion_blocks.block_geoms a
			LEFT JOIN diffusion_shared.urdb_rates_by_state_com b
			ON a.state_abbr = b.state_abbr
			LEFT JOIN diffusion_data_shared.urdb_rates_geoms_com c
			ON b.rate_id_alias = c.rate_id_alias
		),
		b as -- grouping is necessary because rate geoms are exploded (same utility might have several geoms)
		(
			select pgid, rate_id_alias, min(distance_m) as distance_m
			FROM a
			GROUP BY pgid, rate_id_alias
		),
		c as
		(
			SELECT pgid,  rate_id_alias, 
				rank() OVER (partition by pgid ORDER BY distance_m asC) as rank
			FROM b
		)
		select pgid, array_agg(rate_id_alias order by rank) as ranked_rate_ids
		from c
		GROUP BY pgid;',
			'diffusion_blocks.block_urdb_rates_com', 'a', 16);
-----------------------------------------------------------------------------------------------------------
-- add primary key
ALTER TABLE diffusion_blocks.block_urdb_rates_com 
ADD PRIMARY KEY (pgid);

-- check count
select count(*)
FROM diffusion_blocks.block_urdb_rates_com;
-- 10535171

-- check for nulls
select count(*)
FROM diffusion_blocks.block_urdb_rates_com
where ranked_rate_ids = array[null]::INTEGER[];
-- 50641

-- change to actual nulls
UPDATE diffusion_blocks.block_urdb_rates_com
set ranked_rate_ids = NULL
where ranked_rate_ids = array[null]::INTEGER[];

-- recheck
select count(*)
FROM diffusion_blocks.block_urdb_rates_ind
where ranked_rate_ids is null;
-- 50641

-- where are they?
select distinct b.state_abbr
FROM diffusion_blocks.block_urdb_rates_com a
left join diffusion_blocks.block_geoms b
on a.pgid = b.pgid
where a.ranked_rate_ids is null;
-- AK and HI only -- all set
