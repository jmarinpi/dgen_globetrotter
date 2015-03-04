CREATE TABLE dg_wind.existing_distributed_wind_capacity_by_state_2014 (
	state text,
	sector text,
	capacity_mw numeric);

set role 'server-superusers';
COPY dg_wind.existing_distributed_wind_capacity_by_state_2014 FROM '/srv/home/mgleason/data/dg_wind/2014_dg_wind_by_state_and_sector.csv' with csv header;
reset role;

-- add state abbr
ALTER TABLE dg_wind.existing_distributed_wind_capacity_by_state_2014 ADD COLUMN state_abbr character varying(5);

-- this will get most
UPDATE dg_wind.existing_distributed_wind_capacity_by_state_2014 a
SET state_abbr = b.state_abbr
FROM esri.dtl_state_20110101 b
where lower(a.state) = lower(b.state_name);

select distinct(state)
FROM dg_wind.existing_distributed_wind_capacity_by_state_2014
where state_abbr is null;

UPDATE dg_wind.existing_distributed_wind_capacity_by_state_2014
set (state,state_abbr) = ('District of Columbia','DC')
where state = 'DC';

UPDATE dg_wind.existing_distributed_wind_capacity_by_state_2014
set (state,state_abbr) = ('Puerto Rico-Virgin Islands','PR-VI')
where state = 'PR-VI';


-- disaggregate to counties based on proprtion of county load to proportion of state load
DROP TABLE IF EXISTS diffusion_wind.starting_capacities_mw_2014_us;
CREATE TABLE diffusion_wind.starting_capacities_mw_2014_us AS
with sums as (
	SELECT a.state_abbr, 
		sum(b.total_customers_2011_residential) as state_customers_residential, 
		sum(b.total_customers_2011_commercial) as state_customers_commercial, 
		sum(b.total_customers_2011_industrial) as state_customers_industrial
	FROM diffusion_shared.county_geom a
	LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
	ON a.county_id = b.county_id
	where a.state_abbr not in ('AK','HI')
	GROUP BY a.state_abbr
	),
counties as (
	SELECT a.state_abbr, a.county_id,
		b.total_customers_2011_residential,
		b.total_customers_2011_commercial,
		b.total_customers_2011_industrial
	FROM diffusion_shared.county_geom a
	LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
	ON a.county_id = b.county_id
	where a.state_abbr not in ('AK','HI')
	),
capacities as (
	SELECT a.state_abbr, 
		a.capacity_mw as capacity_mw_residential,
		b.capacity_mw as capacity_mw_commercial,
		c.capacity_mw as capacity_mw_industrial,

		a.systems_count as systems_count_residential,
		b.systems_count as systems_count_commercial,
		c.systems_count as systems_count_industrial
				
	FROM dg_wind.existing_distributed_wind_capacity_by_state_2014 a
	left join dg_wind.existing_distributed_wind_capacity_by_state_2014 b
	ON a.state_abbr = b.state_abbr
	left join dg_wind.existing_distributed_wind_capacity_by_state_2014 c
	ON a.state_abbr = c.state_abbr
	where a.sector = 'Residential'
	and b.sector = 'Commercial'
	and c.sector = 'Industrial')
	
SELECT a.state_abbr, a.county_id, 
	a.total_customers_2011_residential/b.state_customers_residential*c.capacity_mw_residential as capacity_mw_residential,
	a.total_customers_2011_commercial/b.state_customers_commercial*c.capacity_mw_commercial as capacity_mw_commercial,
	a.total_customers_2011_industrial/b.state_customers_industrial*c.capacity_mw_industrial as capacity_mw_industrial,

	a.total_customers_2011_residential/b.state_customers_residential*c.systems_count_residential as systems_count_residential,
	a.total_customers_2011_commercial/b.state_customers_commercial*c.systems_count_commercial as systems_count_commercial,
	a.total_customers_2011_industrial/b.state_customers_industrial*c.systems_count_industrial as systems_count_industrial
	
FROM counties a
LEFT JOIN sums b
ON a.state_abbr = b.state_abbr
LEFT JOIN capacities c
ON a.state_abbr = c.state_abbr;

-- create primary key and foreign key
ALTER TABLE diffusion_wind.starting_capacities_mw_2014_us
  ADD CONSTRAINT starting_capacities_mw_2014_us_pkey PRIMARY KEY(county_id);

ALTER TABLE diffusion_wind.starting_capacities_mw_2014_us
  ADD CONSTRAINT county_id FOREIGN KEY (county_id)
      REFERENCES diffusion_shared.county_geom (county_id) MATCH FULL
      ON UPDATE RESTRICT ON DELETE RESTRICT;


-- check results
select state_abbr, 
	round(sum(capacity_mw_residential),2) as res_cap, round(sum(capacity_mw_commercial),2) com_cap, round(sum(capacity_mw_industrial),2) ind_cap,
	round(sum(systems_count_residential),2) res_sys, round(sum(systems_count_commercial),2) com_sys, round(sum(systems_count_industrial),2) ind_sys
FROM diffusion_wind.starting_capacities_mw_2014_us
group by state_abbr
order by state_abbr;
