SET ROLE 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_main_itc_options;
CREATE TABLE diffusion_template.input_main_itc_options
(
  year integer NOT NULL,
  sector text NOT NULL,
  itc_fraction numeric NOT NULL,
  CONSTRAINT input_main_itc_options_year_fkey FOREIGN KEY (year)
	REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE RESTRICT,
  CONSTRAINT itc_fraction_check CHECK (itc_fraction >= 0 and itc_fraction <= 1),
  CONSTRAINT input_main_itc_options_sector_fkey foreign key (sector)
	REFERENCES diffusion_config.sceninp_sector (sector) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE RESTRICT
);

