-- change the table owner
set role 'server-superusers';
ALTER TABLE diffusion_geo.egs_lkup
  OWNER TO "diffusion-writers";
set role 'diffusion-writers';

-- create indices
CREATE INDEX egs_lkup_btree_tract_id_alias
ON diffusion_geo.egs_lkup
USING BTREE(tract_id_alias);

CREATE INDEX egs_lkup_btree_cell_gid
ON diffusion_geo.egs_lkup
USING BTREE(cell_gid);

-- add primary key
ALTER TABLE diffusion_geo.egs_lkup
ADD PRIMARY KEY (tract_id_alias, cell_gid);
-- rejected because of null tract_id_alias

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