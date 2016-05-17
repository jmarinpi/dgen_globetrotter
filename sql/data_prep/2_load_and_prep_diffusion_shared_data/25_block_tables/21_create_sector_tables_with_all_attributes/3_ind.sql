set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_blocks.block_microdata_ind;
CREATE TABLE diffusion_blocks.block_microdata_ind AS
select  a.pgid, 
	m.county_id,
	f.state_abbr,
	f.state_fips,
	f.county_fips,
	m.census_division_abbr,
	m.census_region,
	'p'::text || m.pca_reg::text AS pca_reg, 
	m.reeds_reg,
	d.bldg_probs_ind,
	e.bldg_count_ind,
	ROUND(h.acres_per_bldg, 2) as acres_per_bldg,
	round(p.total_customers_2011_industrial, 2) as county_total_customers_2011,
	round(p.total_load_mwh_2011_industrial, 2) as county_total_load_mwh_2011,
	round(o.pv_20mw_cap_cost_multplier::NUMERIC, 2) as cap_cost_multiplier_solar,
	round(o.onshore_wind_cap_cost_multiplier::NUMERIC, 2) as cap_cost_multiplier_wind,
	b.canopy_ht_m, 
	c.canopy_pct,
	i.solar_re_9809_gid,
	n.i, 
	n.j,
	n.icf/10 as cf_bin,
	k.ulocale,
	l.ranked_rate_ids,
	g.hdf_load_index
from diffusion_blocks.blocks_ind a
LEFT JOIN diffusion_blocks.block_canopy_height b
	ON a.pgid = b.pgid
LEFT JOIN diffusion_blocks.block_canopy_cover c
	ON a.pgid = c.pgid
LEFT JOIN diffusion_blocks.block_bldg_types d
	on a.pgid = d.pgid
LEFT JOIN diffusion_blocks.block_bldg_counts e
	on a.pgid = e.pgid
LEFT JOIN diffusion_blocks.block_geoms f
	on a.pgid = f.pgid
LEFT JOIN  diffusion_blocks.block_load_profile_id_com g
	ON a.pgid = g.pgid
LEFT JOIN diffusion_blocks.block_parcel_size h
	on a.pgid = h.pgid
LEFT JOIN diffusion_blocks.block_resource_id_solar i
	on a.pgid = i.pgid
LEFT JOIN diffusion_blocks.block_resource_id_wind j
	on a.pgid = j.pgid
LEFT JOIN diffusion_blocks.block_ulocale k
	on a.pgid = k.pgid
LEFT JOIN  diffusion_blocks.block_urdb_rates_ind l
	on a.pgid = l.pgid
LEFT JOIN diffusion_blocks.county_geoms m
	on f.county_fips = m.county_fips
	and f.state_fips = m.state_fips
LEFT JOIN aws_2014.iii_jjj_cfbin_raster_lookup n
	on j.iiijjjicf_id = n.raster_value
lEFT JOIN diffusion_shared.capital_cost_multipliers_us o
	ON m.old_county_id = o.county_id
LEFT JOIN diffusion_shared.load_and_customers_by_county_us p
	on m.old_county_id = p.county_id;
-- 945,057 rows

-- row count should be:
select count(*)
FROM diffusion_blocks.blocks_ind;
-- 945,057 -- perfect

-- add primary key
ALTER TABLE diffusion_blocks.block_microdata_ind
ADD PRIMARY KEY (pgid);

-- create indices on:
CREATE INDEX block_microdata_ind_btree_state_abbr
ON diffusion_blocks.block_microdata_ind
USING BTREE(state_abbr);

CREATE INDEX block_microdata_ind_btree_state_fips
ON diffusion_blocks.block_microdata_ind
USING BTREE(state_fips);

CREATE INDEX block_microdata_ind_btree_state_county_fips
ON diffusion_blocks.block_microdata_ind
USING BTREE(county_fips);

-- update stats
VACUUM ANALYZE diffusion_blocks.block_microdata_com;
