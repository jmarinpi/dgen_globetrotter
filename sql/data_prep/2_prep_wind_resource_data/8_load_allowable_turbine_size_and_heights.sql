DROP TABLE IF EXISTS wind_ds.allowable_turbine_sizes;
CREATE TABLE wind_ds.allowable_turbine_sizes (
	turbine_size_kw numeric,
	turbine_height_m integer);

SET ROLE 'server-superusers';
COPY wind_ds.allowable_turbine_sizes FROM '/srv/home/mgleason/data/dg_wind/turbine_sizes_and_heights.csv' with csv header;
RESET ROLE;

-- drop the largest class (3000 kw) -- it is no longer used
DELETE FROM wind_ds.allowable_turbine_sizes
WHERE turbine_size_kw = 3000;

CREATE INDEX allowable_turbine_sizes_turbine_size_kw_btree ON wind_ds.allowable_turbine_sizes using btree(turbine_size_kw);
CREATE INDEX allowable_turbine_sizes_turbine_height_m_btree ON wind_ds.allowable_turbine_sizes using btree(turbine_height_m);


CREATE OR REPLACE VIEW wind_ds.turbine_costs_per_size_and_year AS
SELECT a.turbine_size_kw, a.turbine_height_m, b.year, 
	b.capital_cost_dollars_per_kw * a.turbine_size_kw as capital_cost_dollars,
	b.fixed_om_dollars_per_kw_per_yr * a.turbine_size_kw as fixed_om_dollars_per_year,
	b.variable_om_dollars_per_kwh,
	b.cost_for_higher_towers * 1000 * (a.turbine_height_m-b.default_tower_height_m) as tower_cost_adder_dollars
FROM wind_ds.allowable_turbine_sizes a
lEFT JOIN wind_ds.wind_cost_projections b
ON a.turbine_size_kw = b.turbine_size_kw;
