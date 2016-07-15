set role 'diffusion-writers';

DROP TABLE if exists diffusion_shared.aeo_new_building_multipliers_2015;
CREATE TABLE diffusion_shared.aeo_new_building_multipliers_2015 AS
select state_fips, state, census_division, year, scenario, 
	1.02::NUMERIC as res_single_family_growth, 
        1.02::NUMERIC AS res_multi_family_growth, 
        1.02::NUMERIC AS com_growth, 
       state_abbr, census_division_abbr
from  diffusion_shared.aeo_new_building_projections_2015;

-- add primary key on state, year, scenario
ALTER TABLE diffusion_shared.aeo_new_building_multipliers_2015
ADD PRIMARY KEY (state_abbr, year, scenario);

-- add indices
create INDEX aeo_new_building_multipliers_2015_btree_state_abbr
on diffusion_shared.aeo_new_building_multipliers_2015
using btree(state_abbr);

create INDEX aeo_new_building_multipliers_2015_btree_year
on diffusion_shared.aeo_new_building_multipliers_2015
using btree(year);

create INDEX aeo_new_building_multipliers_2015_btree_scenario
on diffusion_shared.aeo_new_building_multipliers_2015
using btree(scenario);

-- change the names for the scenarios to our standard naming conventions
select distinct scenario
from diffusion_shared.aeo_new_building_multipliers_2015;
-- Reference --> AEO2015 Reference
-- High Growth --> AEO2015  High Growth
-- Low Growth --> AEO2015 Low Growth
-- Low Price --> AEO2015 Low Prices
-- High Price --> AEO2015 High Prices

UPDATE diffusion_shared.aeo_new_building_multipliers_2015
set scenario = 
	CASE
		WHEN scenario = 'Reference' THEN 'AEO2015 Reference'
		WHEN scenario = 'High Growth' THEN 'AEO2015 High Growth'
		WHEN scenario = 'Low Growth' THEN 'AEO2015 Low Growth'
		WHEN scenario = 'Low Price' THEN 'AEO2015 Low Prices'
		WHEN scenario = 'High Price' THEN 'AEO2015 High Prices'
	end;

-- check results
select distinct scenario
from diffusion_shared.aeo_new_building_multipliers_2015;