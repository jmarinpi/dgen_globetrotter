set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_shared.aeo_rate_escalations_2015;
CREATE TABLE diffusion_shared.aeo_rate_escalations_2015
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

\COPY diffusion_shared.aeo_rate_escalations_2015 FROM '/Volumes/Staff/mgleason/DG_Solar/Data/Source_Data/aeo_2015/rate_escalations.csv' with csv header;


CREATE INDEX aeo_rate_escalations_2015_btree_census_division_abbr
  ON diffusion_shared.aeo_rate_escalations_2015
  USING btree
  (census_division_abbr COLLATE pg_catalog."default");

CREATE INDEX aeo_rate_escalations_2015_btree_sector
  ON diffusion_shared.aeo_rate_escalations_2015
  USING btree
  (sector COLLATE pg_catalog."default");

CREATE INDEX aeo_rate_escalations_2015_btree_sector_abbr
  ON diffusion_shared.aeo_rate_escalations_2015
  USING btree
  (sector_abbr COLLATE pg_catalog."default");

CREATE INDEX aeo_rate_escalations_2015_btree_source
  ON diffusion_shared.aeo_rate_escalations_2015
  USING btree
  (source COLLATE pg_catalog."default");

CREATE INDEX aeo_rate_escalations_2015_btree_year
  ON diffusion_shared.aeo_rate_escalations_2015
  USING btree
  (year);

-- check against old data
select *
from diffusion_shared.aeo_rate_escalations_2015 a
left join diffusion_shared.rate_escalations b
ON a.year = b.year
and a.sector_abbr = b.sector_abbr
and regexp_replace(a.source, '2015', '2014') = b.source
and a.census_division_abbr = b.census_division_abbr
where b.year is null;
-- only ones missing are aoe2015 extended years 2012 and 2013 (these are missing in the older data for some reason)