-- how many utility districts are represented in the urdb data we collected
SELECT distinct(ur_name) as ur_name
FROM urdb_rates.urdb3_verified_rates_sam_data_20141202;
-- 243

-- see how many unique match to a single ventyx boundary
with a AS
(
	SELECT distinct(ur_name) as ur_name
	FROM urdb_rates.urdb3_verified_rates_sam_data_20141202 
),
b as 
(
	select a.ur_name, b.company_na, b.company_id
	FROM a
	LEFT JOIN ventyx.electric_service_territories_20130422 b
	ON a.ur_name = b.company_na
	where b.company_na is not null
),
c as 
(
	-- have to do this because polygons are singlepart
	SELECT ur_name, company_id
	FROM b
	GROUP BY ur_name, company_id
)
SElECT ur_name, count(company_id)
FROM c
group by ur_name
order by count desc;
-- 87 of them


-- how many do not have a match on name?
with a AS
(
	SELECT distinct(ur_name) as ur_name
	FROM urdb_rates.urdb3_verified_rates_sam_data_20141202 
)
select a.ur_name, b.company_na, b.company_id
FROM a
LEFT JOIN ventyx.electric_service_territories_20130422 b
ON a.ur_name = b.company_na
where b.company_na is null
;
-- 156 of them

select 156+87; -- 243

-- a few solutions:
-- use the urdb to eia id lookup...
-- use fuzzy matching


