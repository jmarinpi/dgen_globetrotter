set role mgleason;
GRANT ALL ON pv_rooftop_dsolar_integration.percent_developable_buildings TO "diffusion-writers";
GRANT ALL ON pv_rooftop_dsolar_integration.rooftop_orientation_frequencies_optimal_only TO "diffusion-writers";
GRANT ALL ON pv_rooftop_dsolar_integration.rooftop_orientation_frequencies_optimal_blended TO "diffusion-writers";
GRANT ALL ON pv_rooftop_dsolar_integration.lidar_city_ulocale_zone_size_class_lkup TO "diffusion-writers";
ALTER TABLE diffusion_data_shared.county_ranked_lidar_city_lkup_res OWNER TO "diffusion-writers";
ALTER TABLE diffusion_data_shared.county_ranked_lidar_city_lkup_com OWNER TO "diffusion-writers";
ALTER TABLE diffusion_data_shared.county_ranked_lidar_city_lkup_ind OWNER TO "diffusion-writers";

set role 'diffusion-writers';

------------------------------------------------------------------------------------------
-- percent of buildings that are developable
------------------------------------------------------------------------------------------
-- by city, sector, and size class
DROP TABLE IF EXISTS diffusion_solar.rooftop_percent_developable_buildings;
CREATE TABLE diffusion_solar.rooftop_percent_developable_buildings AS
SELECT city_id, zone, size_class, pct_developable
FROM pv_rooftop_dsolar_integration.percent_developable_buildings;

-- create indices
-- city id
CREATE INDEX rooftop_percent_developable_buildings_btree_city_id
ON diffusion_solar.rooftop_percent_developable_buildings
using BTREE(city_id);

-- zone
CREATE INDEX rooftop_percent_developable_buildings_btree_zone
ON diffusion_solar.rooftop_percent_developable_buildings
using BTREE(zone);

-- size class
CREATE INDEX rooftop_percent_developable_buildings_btree_size_class
ON diffusion_solar.rooftop_percent_developable_buildings
using BTREE(size_class);

------------------------------------------------------------------------------------------
-- discrete distributions of optimal plane orientations
------------------------------------------------------------------------------------------
-- OPTIMAL ONLY
DROP TABLE IF EXISTS diffusion_solar.rooftop_orientation_frequencies_optimal_only;
CREATE TABLE diffusion_solar.rooftop_orientation_frequencies_optimal_only AS
SELECT *
FROM pv_rooftop_dsolar_integration.rooftop_orientation_frequencies_optimal_only;

-- create indices
CReATE INDEX rooftop_orientation_frequencies_optimal_only_zone_btree
ON diffusion_solar.rooftop_orientation_frequencies_optimal_only
USING BTREE (zone);

CReATE INDEX rooftop_orientation_frequencies_optimal_only_size_class_btree
ON diffusion_solar.rooftop_orientation_frequencies_optimal_only
USING BTREE (size_class);

CReATE INDEX rooftop_orientation_frequencies_optimal_only_ulocale_btree
ON diffusion_solar.rooftop_orientation_frequencies_optimal_only
USING BTREE (ulocale);

CReATE INDEX rooftop_orientation_frequencies_optimal_only_city_id_btree
ON diffusion_solar.rooftop_orientation_frequencies_optimal_only
USING BTREE (city_id);


-- OPTIMAL BLENDED
DROP TABLE IF EXISTS diffusion_solar.rooftop_orientation_frequencies_optimal_blended;
CREATE TABLE diffusion_solar.rooftop_orientation_frequencies_optimal_blended AS
SELECT *
FROM pv_rooftop_dsolar_integration.rooftop_orientation_frequencies_optimal_blended;

-- create indices
CReATE INDEX rooftop_orientation_frequencies_optimal_blended_zone_btree
ON diffusion_solar.rooftop_orientation_frequencies_optimal_blended
USING BTREE (zone);

CReATE INDEX rooftop_orientation_frequencies_optimal_blended_size_class_btree
ON diffusion_solar.rooftop_orientation_frequencies_optimal_blended
USING BTREE (size_class);

CReATE INDEX rooftop_orientation_frequencies_optimal_blended_ulocale_btree
ON diffusion_solar.rooftop_orientation_frequencies_optimal_blended
USING BTREE (ulocale);

CReATE INDEX rooftop_orientation_frequencies_optimal_blended_city_id_btree
ON diffusion_solar.rooftop_orientation_frequencies_optimal_blended
USING BTREE (city_id);

------------------------------------------------------------------------------------------
-- lookup for ulocale, zone, and size class by City
------------------------------------------------------------------------------------------
CREATE TABLE diffusion_solar.rooftop_city_ulocale_zone_size_class_lkup AS
SELECT *
FROM pv_rooftop_dsolar_integration.lidar_city_ulocale_zone_size_class_lkup;

-- create indices
CREATE INDEX lidar_city_ulocale_zone_size_class_lkup_city_id_btree
ON diffusion_solar.rooftop_city_ulocale_zone_size_class_lkup
USING BTREE(city_id);

CREATE INDEX lidar_city_ulocale_zone_size_class_lkup_zone_btree
ON diffusion_solar.rooftop_city_ulocale_zone_size_class_lkup
USING BTREE(zone);

CREATE INDEX lidar_city_ulocale_zone_size_class_lkup_ulocale_btree
ON diffusion_solar.rooftop_city_ulocale_zone_size_class_lkup
USING BTREE(ulocale);

CREATE INDEX lidar_city_ulocale_zone_size_class_lkup_size_class_btree
ON diffusion_solar.rooftop_city_ulocale_zone_size_class_lkup
USING BTREE(size_class);

------------------------------------------------------------------------------------------
-- Ranks for Cities for each County/Ulocale cross-section
-- (ranks are based on shortest distance first, then most recent year)
------------------------------------------------------------------------------------------
-- res
ALTER TABLE diffusion_data_shared.county_ranked_lidar_city_lkup_res
RENAME TO rooftop_city_ranks_by_county_and_ulocale_res;

ALTER TABLE diffusion_data_shared.rooftop_city_ranks_by_county_and_ulocale_res
SET SCHEMA diffusion_solar;

-- com
ALTER TABLE diffusion_data_shared.county_ranked_lidar_city_lkup_com
RENAME TO rooftop_city_ranks_by_county_and_ulocale_com;

ALTER TABLE diffusion_data_shared.rooftop_city_ranks_by_county_and_ulocale_com
SET SCHEMA diffusion_solar;

-- ind
ALTER TABLE diffusion_data_shared.county_ranked_lidar_city_lkup_ind
RENAME TO rooftop_city_ranks_by_county_and_ulocale_ind;

ALTER TABLE diffusion_data_shared.rooftop_city_ranks_by_county_and_ulocale_ind
SET SCHEMA diffusion_solar;
------------------------------------------------------------------------------------------
