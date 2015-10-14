-- load the aeo 2014 extended data from ben

set role 'diffusion-writers';
DROP TABLE IF EXISTS diffusion_shared.rate_escalations_extended;
CREATE TABLE diffusion_shared.rate_escalations_extended 
(
  census_division_abbr text,
  sector text,
  year integer,
  escalation_factor numeric,
  source text,
  sector_abbr character varying(3),
  CONSTRAINT sector_abbr_check CHECK (sector_abbr::text = ANY (ARRAY['res'::character varying::text, 'com'::character varying::text, 'ind'::character varying::text])),
  CONSTRAINT sector_check CHECK (sector = ANY (ARRAY['Residential'::text, 'Commercial'::text, 'Industrial'::text]))
);

\COPY diffusion_shared.rate_escalations_extended FROM '/Volumes/Staff/mgleason/DG_Wind/Data/Source_Data/AEO/rate_esc_by_state_and_sector_extended_20150901.csv' with csv header;


-- change the source to AEO2014_Extended
UPDATE diffusion_shared.rate_escalations_extended 
SET source = 'AEO2014 Extended';

-- the main difference between this and the old table is that the old unextended version
-- includes data for 2011 - 2013 (which isn't needed in the model).
-- otherwise, the only differences that occur are after 2040:
select a.census_division_abbr, a.sector, a.year, a.escalation_factor, b.escalation_factor
from diffusion_shared.rate_escalations a
INNER join diffusion_shared.rate_escalations_extended b
ON a.census_division_abbr = b.census_division_abbr
and a.sector = b.sector
and a.year = b.year
where round(a.escalation_factor, 4) <> round(b.escalation_factor, 4)
order by year; 

-- insert rate_escalations_extended into rate_escalations
INSERT INTO diffusion_shared.aeo_rate_escalations_2014
select *
FROM diffusion_shared.rate_escalations_extended;
-- 1809 rows

-- drop rate_escalations_extended
DROP TABLE IF EXISTS diffusion_shared.rate_escalations_extended;

-- make AEO2014_Extended an allowable option in scenario_inputs
INSERT INTO diffusion_config.sceninp_rate_escalation
values ('AEO2014 Extended');

