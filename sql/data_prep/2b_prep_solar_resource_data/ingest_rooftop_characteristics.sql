--- load table with various characteristics and probabilities
set role 'diffusion-writers';
DROP tABLE IF EXISTS diffusion_solar.rooftop_characteristics;
CREATE TABLE diffusion_solar.rooftop_characteristics
(
	sector_abbr character varying(3),
	tilt integer,
	azimuth character varying(2),
	prob_weight numeric,
	roof_style text,
	roof_planes integer,
	rooftop_portion numeric,
	slope_area_multiplier numeric,
	unshaded_multiplier numeric
);

set role 'server-superusers';
COPY diffusion_solar.rooftop_characteristics 
FROM '/home/mgleason/data/dg_solar/roof_orientations_updated.csv' with csv header;
set role 'diffusion-writers';

-- add indices
CREATE INDEX rooftop_characteristics_sector_abbr_btree
ON diffusion_solar.rooftop_characteristics 
using btree(sector_abbr);

CREATE INDEX rooftop_characteristics_roof_style_btree
ON diffusion_solar.rooftop_characteristics 
using btree(roof_style);

--- load lookup table for CBECS roof material to roof style
set role 'diffusion-writers';
DROP tABLE IF EXISTS diffusion_solar.roof_material_to_roof_style_cbecs;
CREATE TABLE diffusion_solar.roof_material_to_roof_style_cbecs
(
	rcfns integer,
	description text,
	roof_style text
);

set role 'server-superusers';
COPY diffusion_solar.roof_material_to_roof_style_cbecs 
FROM '/home/mgleason/data/dg_solar/cbecs_roof_material_to_roof_style.csv' with csv header;
set role 'diffusion-writers';

-- add indices
CREATE INDEX roof_material_to_roof_style_cbecs_rcfns_btree
ON diffusion_solar.roof_material_to_roof_style_cbecs 
using btree(rcfns);

CREATE INDEX roof_material_to_roof_style_cbecs_roof_style_btree
ON diffusion_solar.roof_material_to_roof_style_cbecs 
using btree(roof_style);

--- load lookup table for RECS roof material to roof style
set role 'diffusion-writers';
DROP tABLE IF EXISTS diffusion_solar.roof_material_to_roof_style_recs;
CREATE TABLE diffusion_solar.roof_material_to_roof_style_recs
(
	rooftype integer,
	description text,
	roof_style text
);

set role 'server-superusers';
COPY diffusion_solar.roof_material_to_roof_style_recs 
FROM '/home/mgleason/data/dg_solar/recs_roof_material_to_roof_style.csv' with csv header;
set role 'diffusion-writers';

-- add indices
CREATE INDEX roof_material_to_roof_style_recs_rcfns_btree
ON diffusion_solar.roof_material_to_roof_style_recs 
using btree(rooftype);

CREATE INDEX roof_material_to_roof_style_recs_roof_style_btree
ON diffusion_solar.roof_material_to_roof_style_recs 
using btree(roof_style);

