-- copy the rate sam_json data over to diffusion_shared with the rate alias id
set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_shared.urdb3_rate_sam_jsons CASCADE;
CREATE TABLE diffusion_shared.urdb3_rate_sam_jsons AS
with a AS
(
	SELECT distinct(rate_id_alias)
	FROM urdb_rates.combined_singular_verified_rates_lookup
)
SELECT b.rate_id_alias, b.sam_json
FROM  urdb_rates.urdb3_singular_rates_sam_data_20141202 b
inner join urdb_rates.urdb3_singular_rates_lookup_20141202 c
ON b.rate_id_alias = c.rate_id_alias
and c.verified = False
INNER JOIN a
on a.rate_id_alias = b.rate_id_alias
UNION ALL -- union all is ok as long as the count of output rows matches the counnt of rows in a
SELECT b.rate_id_alias, b.sam_json
FROM  urdb_rates.urdb3_verified_rates_sam_data_20141202 b
INNER JOIN a
on a.rate_id_alias = b.rate_id_alias;

-- add primary key on rate_id_alias
ALTER TABLE diffusion_shared.urdb3_rate_sam_jsons
add primary key(rate_id_alias);