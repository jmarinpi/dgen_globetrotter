﻿set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_blocks.block_microdata_res CASCADE;
CREATE TABLE diffusion_blocks.block_microdata_res AS
select  a.pgid, 
	m.county_id,
	f.state_abbr,
	f.state_fips,
	f.county_fips,
	f.tract_fips,
	r.tract_id_alias,
	m.old_county_id,
	m.census_division_abbr,
	m.census_region,
	m.reportable_domain,
	'p'::text || m.pca_reg::text AS pca_reg, 
	m.reeds_reg,
	d.bldg_probs_res,
	e.bldg_count_res,
	d.bldg_probs_res[1] + d.bldg_probs_res[2] as bldg_count_single_fam_res,
	ROUND(h.acres_per_bldg, 2) as acres_per_bldg,
	round(p.total_customers_2011_residential * q.perc_own_occu_1str_housing, 2) as county_total_customers_2011,
	round(p.total_load_mwh_2011_residential * q.perc_own_occu_1str_housing, 2) as county_total_load_mwh_2011,
	round(o.pv_20mw_cap_cost_multplier::NUMERIC, 2) as cap_cost_multiplier_solar,
	round(o.onshore_wind_cap_cost_multiplier::NUMERIC, 2) as cap_cost_multiplier_wind,
	b.canopy_ht_m, 
	c.canopy_pct,
	i.solar_re_9809_gid,
	n.i, 
	n.j,
	n.icf/10 as cf_bin,
	k.ulocale,
	l.rate_ids,
	l.rate_ranks,
	g.hdf_load_index,
	s.climate_zone_building_america as climate_zone_recs,
	s.climate_zone_cbecs_2003 as climate_zone_cbecs,
	t.climate_zone as iecc_temperature_zone,
	t.moisture_regime as iecc_moisture_regime,
	t.climate_zone::TExT || t.moisture_regime as iecc_climate_zone,
	u.utility_type_res as utility_type
from diffusion_blocks.blocks_res a
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
LEFT JOIN  diffusion_blocks.block_load_profile_id_res g
	ON a.pgid = g.pgid
LEFT JOIN diffusion_blocks.block_parcel_size h
	on a.pgid = h.pgid
LEFT JOIN diffusion_blocks.block_resource_id_solar i
	on a.pgid = i.pgid
LEFT JOIN diffusion_blocks.block_resource_id_wind j
	on a.pgid = j.pgid
LEFT JOIN diffusion_blocks.block_ulocale k
	on a.pgid = k.pgid
LEFT JOIN  diffusion_blocks.block_urdb_rates_res l
	on a.pgid = l.pgid
LEFT JOIN diffusion_blocks.county_geoms m
	on f.county_fips = m.county_fips
	and f.state_fips = m.state_fips
LEFT JOIN aws_2014.iii_jjj_cfbin_raster_lookup n
	on j.iiijjjicf_id = n.raster_value
lEFT JOIN diffusion_shared.capital_cost_multipliers_us o
	ON m.old_county_id = o.county_id
LEFT JOIN diffusion_shared.load_and_customers_by_county_us p
	on m.old_county_id = p.county_id
lEFT JOIN diffusion_shared.county_housing_units q
	on m.old_county_id = q.county_id
LEFT JOIN diffusion_blocks.block_tract_id_alias r
	ON a.pgid = r.pgid
LEFT JOIN diffusion_shared.county_geom s
	on m.old_county_id = s.county_id
LEFT JOIN ashrae.county_to_iecc_building_climate_zones_lkup t
	on m.county_fips = t.county_fips
	and m.state_fips = t.state_fips
LEFT JOIN diffusion_blocks.block_primary_electric_utilities u
	ON a.pgid = u.pgid;
-- 6,251,958 rows

-- row count should be:
select count(*)
FROM diffusion_blocks.blocks_res;
-- 6,251,958 -- perfect

-- add primary key
ALTER TABLE diffusion_blocks.block_microdata_res
ADD PRIMARY KEY (pgid);

-- create indices on:
CREATE INDEX block_microdata_res_btree_state_abbr
ON diffusion_blocks.block_microdata_res
USING BTREE(state_abbr);

CREATE INDEX block_microdata_res_btree_state_fips
ON diffusion_blocks.block_microdata_res
USING BTREE(state_fips);

CREATE INDEX block_microdata_res_btree_county_fips
ON diffusion_blocks.block_microdata_res
USING BTREE(county_fips);

CREATE INDEX block_microdata_res_btree_county_id
ON diffusion_blocks.block_microdata_res
USING BTREE(county_id);

CREATE INDEX block_microdata_res_btree_tract_id_alias
ON diffusion_blocks.block_microdata_res
USING BTREE(tract_id_alias);

CREATE INDEX block_microdata_res_btree_iecc_climate_zone
ON diffusion_blocks.block_microdata_res
USING BTREE(iecc_climate_zone);

-- update stats
vACUUM ANALYZE diffusion_blocks.block_microdata_res;