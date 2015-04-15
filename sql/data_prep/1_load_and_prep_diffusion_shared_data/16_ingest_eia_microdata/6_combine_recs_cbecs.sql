
﻿---- Creates combined recs and cbecs views with additional columns to help in where clauses within the Python code
---- Created on 4-14-15 by Jon Duckworth

SET ROLE 'diffusion-writers'
DROP VIEW IF EXISTS diffusion_shared.cbecs_recs_combined;

CREATE OR REPLACE VIEW diffusion_shared.cbecs_recs_combined
AS
SELECT c.pubid8 as load_id,
c.adjwt8 as weight,
c.elcns8 as ann_cons_kwh,
c.crb_model as crb_model,
c.roof_style as roof_style,
c.roof_sqft as roof_sqft,
c.ownocc8 as ownocc8,
'diffusion_shared.eia_microdata_cbecs_2003' as source_table,
ARRAY['res'] as sector,
pba8 <> 1 as sample_cust_and_load_selector,
c.census_division_abbr as census_division_abbr,
NULL as reportable_domain
FROM diffusion_shared.eia_microdata_cbecs_2003 as c
UNION
SELECT r.doeid as load_id,
r.nweight as weight,
r.kwh as ann_cons_kwh,
r.crb_model as crb_model,
r.roof_style as roof_style,
r.roof_sqft as roof_sqft,
1 as ownocc8,
'diffusion_shared.eia_microdata_recs_2009' as source_table,
ARRAY['com', 'ind'] as sector,
(typehuq in (1,2,3) AND kownrent = 1) as sample_cust_and_load_selector,
r.reportable_domain::text as reportable_domain,
NULL as census_division_abbr
FROM diffusion_shared.eia_microdata_recs_2009 as r
;

COMMENT ON VIEW diffusion_shared.cbecs_recs_combined IS '''
Combined data from eia_microdata_cbecs_2003 and eia_microdata_recs_2009 with standardized field names. 
See column-level comments for relationship to original field names
''';

COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.load_id IS 'recs.doeid or cbecs.pubid8';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.weight IS 'recs.nweight or cbecs.adjwt8';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.ann_cons_kwh IS 'recs.kwh or cbecs.elcns8';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.crb_model IS 'recs.crb_model or cbecs.crb_model';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.roof_style IS 'recs.roof_style or cbecs.roof_style';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.roof_sqft IS 'recs.roof_sqft or cbecs.roof_sqft';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.ownocc8 IS 'recs.ownocc8 or 1 for cbecs';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.source_table IS 'recs or cbecs source table';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.sector IS 'Corresponds to sector_abbr in Python scripts';
COMMENT ON COLUMN diffusion_shared.cbecs_recs_combined.sample_cust_and_load_selector IS '''
limit to mobilehomes (1) single-family (3 - attached or 2 - detached), owner-occupied homes for recs
limit to non-vacant buildings for cbecs
''';
