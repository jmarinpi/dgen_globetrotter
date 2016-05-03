set role 'dgeo-writers';

-- add primary keys to tables
ALTER TABLE dgeo.egs_accessible_resource_by_depth 
ADD PRIMARY KEY (gid);

ALTER TABLE dgeo.egs_accessible_resource_total
ADD PRIMARY KEY (gid);

