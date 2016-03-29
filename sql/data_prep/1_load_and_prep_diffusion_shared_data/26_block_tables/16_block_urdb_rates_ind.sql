-- diffusion_shared.urdb_rates_by_state_res
-- diffusion_shared.urdb_rates_by_state_com
-- diffusion_shared.urdb_rates_by_state_ind


set role 'diffusion-writers';

-----------------------------------------------------------------------------------------------------------
-- commercial 
DROP TABLE IF EXISTS diffusion_blocks.block_urdb_rates_ind;
CReATE TABLE diffusion_blocks.block_urdb_rates_ind 
(
	pgid bigint,
	ranked_rate_ids integer[]
);

select parsel_2('dav-gis', 'mgleason', 'mgleason',
		'diffusion_blocks.block_geoms', 'pgid',
		'with x as
		(
			select *
			from diffusion_data_shared.urdb_rates_geoms_com
			union all
			select *
			FROM diffusion_data_shared.urdb_rates_geoms_ind
		),
		a as
		(
			SELECT a.pgid, 
				x.rate_id_alias, 
				ST_Distance(a.the_point_96703, x.the_geom_96703) as distance_m
			FROM diffusion_blocks.block_geoms a
			LEFT JOIN diffusion_shared.urdb_rates_by_state_ind b
			ON a.state_abbr = b.state_abbr
			LEFT JOIN x
			ON b.rate_id_alias = x.rate_id_alias
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
			'diffusion_blocks.block_urdb_rates_ind', 'a', 16);
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
where ranked_rate_ids is null;
-- 50641

-- where are they?
select distinct b.state_abbr
FROM diffusion_blocks.block_urdb_rates_com a
left join diffusion_blocks.block_geoms b
on a.pgid = b.pgid
where a.ranked_rate_ids is null;
-- AK and HI only -- all set