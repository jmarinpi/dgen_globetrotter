-- add a primary key
ALTER TABLE urdb_rates.urdb3_singular_rates_sam_data_20141202
ADD PRIMARY KEY (urdb_rate_id);

-- add columns for applicability to lookup table
ALTER TABLE urdb_rates.urdb3_singular_rates_lookup_20141202
DROP column demand_min numeric,
ADD column demand_max numeric;

select *
FROM urdb_rates.urdb3_verified_rates_lookup_20141202
where res_com = 'C'
order by utility_name;

select *
FROM urdb_rates.urdb3_verified_rates_lookup_20141202
where res_com = 'C'
order by utility_name;

select *
FROM urdb_rates.urdb3_verified_rates_sam_data_20141202
where urdb_rate_id = '539f73c3ec4f024411ed0011'


select *
FROM urdb_rates.urdb3_verified_rates_lookup_20141202
where res_com = 'R'
order by utility_name;

select *
FROM  urdb_rates.urdb3_verified_rates_lookup_20141202
where res_com = 'R'
and demand_max <= 25

-- for residential, we will ignore demand thresholds