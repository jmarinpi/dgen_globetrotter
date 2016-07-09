-- change the table owner
set role 'server-superusers';
ALTER TABLE diffusion_geo.hydro_poly_tracts
  OWNER TO "diffusion-writers";
ALTER TABLE diffusion_geo.hydro_pt_tracts
  OWNER TO "diffusion-writers";
set role 'diffusion-writers';



-- create indices
CREATE INDEX hydro_poly_tracts_btree_tract_id_alias
ON diffusion_geo.hydro_poly_tracts
USING BTREE(tract_id_alias);

CREATE INDEX hydro_pt_tracts_btree_tract_id_alias
ON diffusion_geo.hydro_pt_tracts
USING BTREE(tract_id_alias);

CREATE INDEX hydro_poly_tracts_btree_resource_uid
ON diffusion_geo.hydro_poly_tracts
USING BTREE(resource_uid);

CREATE INDEX hydro_pt_tracts_btree_resource_uid
ON diffusion_geo.hydro_pt_tracts
USING BTREE(resource_uid);

-- add primary key
ALTER TABLE diffusion_geo.hydro_pt_tracts
ADD PRIMARY KEY (tract_id_alias, resource_uid);
-- rejected beacuse tract_id_alias has nulls

ALTER TABLE diffusion_geo.hydro_poly_tracts
ADD PRIMARY KEY (tract_id_alias, resource_uid);
-- rejected because of duplicate combination

-- fix these issues
-- POINTS
select *
FROM diffusion_geo.hydro_pt_tracts
where tract_id_alias is null;
-- two in CA, two in AK -- assume CA are offshore, so just delete

DELETE
FROM diffusion_geo.hydro_pt_tracts
where tract_id_alias is null;
-- 4 rows deleted

-- add primary key
ALTER TABLE diffusion_geo.hydro_pt_tracts
ADD PRIMARY KEY (tract_id_alias, resource_uid);
-- rejected again due to duplicates

SELECT *
FROM diffusion_geo.hydro_pt_tracts
where tract_id_alias = 16353
and resource_uid = 'OR145'

select *
FROM diffusion_geo.resources_hydrothermal_pt
where uid = 'OR145'

set role 'server-superusers';
select *
FROm diffusion_geo.hydro_pt_lkup
order by tract_id_alias, resource_uid
where tract_id_alias = 16353
and resource_uid = 'OR145'
-- add the primary key

-- why are they null?
select cell_gid
FROM diffusion_geo.egs_lkup
where tract_id_alias is null
group by cell_gid;
-- reviewed in Q and they are all coastal, which is fine

-- delte them
DELETE 
FROM diffusion_geo.egs_lkup
where tract_id_alias is null;
-- 100 deleted

-- change the table name
ALTER TABLE diffusion_geo.egs_lkup
RENAME TO egs_tract_id_alias_lkup;

-- add the primary key
ALTER TABLE diffusion_geo.egs_tract_id_alias_lkup
ADD PRIMARY KEY (tract_id_alias, cell_gid);

-- change the cell gid to integfer
ALTER TABLE diffusion_geo.egs_tract_id_alias_lkup
ALTER COLUMN cell_gid type integer using cell_gid::INTEGER;