﻿SET ROLE 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_main_itc_solar;
CREATE TABLE diffusion_template.input_main_itc_solar
(
  year integer NOT NULL,
  sector text NOT NULL,
  itc_fraction numeric NOT NULL,
  CONSTRAINT input_main_itc_solar_year_fkey FOREIGN KEY (year)
	REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE RESTRICT,
  CONSTRAINT itc_fraction_check CHECK (itc_fraction >= 0 and itc_fraction <= 1),
  CONSTRAINT input_main_itc_solar_sector_fkey foreign key (sector)
	REFERENCES diffusion_config.sceninp_sector (sector) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_main_itc_small_wind;
CREATE TABLE diffusion_template.input_main_itc_small_wind
(
  year integer NOT NULL,
  sector text NOT NULL,
  itc_fraction numeric NOT NULL,
  CONSTRAINT input_main_itc_small_wind_year_fkey FOREIGN KEY (year)
	REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE RESTRICT,
  CONSTRAINT itc_fraction_check CHECK (itc_fraction >= 0 and itc_fraction <= 1),
  CONSTRAINT input_main_itc_small_wind_sector_fkey foreign key (sector)
	REFERENCES diffusion_config.sceninp_sector (sector) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_main_itc_large_wind;
CREATE TABLE diffusion_template.input_main_itc_large_wind
(
  year integer NOT NULL,
  sector text NOT NULL,
  itc_fraction numeric NOT NULL,
  CONSTRAINT input_main_itc_large_wind_year_fkey FOREIGN KEY (year)
	REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE RESTRICT,
  CONSTRAINT itc_fraction_check CHECK (itc_fraction >= 0 and itc_fraction <= 1),
  CONSTRAINT input_main_itc_large_wind_sector_fkey foreign key (sector)
	REFERENCES diffusion_config.sceninp_sector (sector) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE RESTRICT
);


-- DROP TABLE IF EXISTS diffusion_template.input_main_itc_options;
DROP VIEW IF EXISTS diffusion_template.input_main_itc_options;
CREATE VIEW diffusion_template.input_main_itc_options AS
SELECT *, 'solar'::TEXT as tech, 
	-1::DOUBLE PRECISION as min_kw, 'Inf'::DOUBLE PRECISION as max_kw
from diffusion_template.input_main_itc_solar
UNION ALL
SELECT *, 'wind'::TEXT as tech, 
	-1::DOUBLE PRECISION as min_kw, 100::DOUBLE PRECISION as max_kw
from diffusion_template.input_main_itc_small_wind
UNION ALL
SELECT *, 'wind'::TEXT as tech, 
	100::DOUBLE PRECISION as min_kw, 'Inf'::DOUBLE PRECISION as max_kw
from diffusion_template.input_main_itc_large_wind;


