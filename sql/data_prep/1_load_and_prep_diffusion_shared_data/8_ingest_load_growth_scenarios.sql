DROP TABLE IF EXISTS diffusion_shared.aeo_load_growth_projections;
CREATE TABLE diffusion_shared.aeo_load_growth_projections
(
  census_division_abbr character varying(3),
  scenario text,
  year integer,
  load_multiplier numeric
);

SET ROLE 'server-superusers';
COPY diffusion_shared.aeo_load_growth_projections FROM '/srv/home/mgleason/data/dg_wind/AEO_Load_Growth_Projection.csv' WITH CSV HEADER;
RESET ROLE;

CREATE INDEX aeo_load_growth_projections_year_btree ON diffusion_shared.aeo_load_growth_projections USING btree(year);
CREATE INDEX aeo_load_growth_projections_census_division_abbr_btree ON diffusion_shared.aeo_load_growth_projections USING btree(census_division_abbr);
CREATE INDEX aeo_load_growth_projections_scenario_btree ON diffusion_shared.aeo_load_growth_projections USING btree(scenario);
