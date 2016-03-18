set role 'dgeo-writers';
DROP TABLE IF EXISTS dgeo.hazus_blocks_to_census2010_blocks;
CREATE TABLE dgeo.hazus_blocks_to_census2010_blocks
(
	state_abbr varchar(2),
	blocks_in_census_missing_from_hazus integer,
	blocks_in_hazus_missing_from_census integer
);

DO LANGUAGE plpgsql $$
	-- get the state ids
	DECLARE recs CURSOR FOR 
				select table_name, right(table_name, 2) as state_abbr
				from tablenames('census_2010') t
				WHERE t like 'block_geom_%'
				and t <> 'block_geom_pr'
				AND length(t) = 13;
	BEGIN
		for rec in recs loop
			execute 
			'INSERT INTO dgeo.hazus_blocks_to_census2010_blocks
			
			 SELECT ''' || rec.state_abbr || ''' AS state_abbr, 
				sum((b.censusblock is null)::INTEGER), 
				sum((a.geoid10 is null)::INTEGER)
			from census_2010.block_geom_' || rec.state_abbr || ' a
			FULL OUTER JOIN hazus.hzbldgcountoccupb_' || rec.state_abbr || ' b
			ON a.geoid10 = b.censusblock
			WHERE a.aland10 > 0
			AND (b.res1i + b.res2i + b.res3ai + b.res3bi + 
				b.res3ci + b.res3di + b.res3ei + b.res3fi + 
				b.res4i + b.res5i + b.res6i + b.com1i + 
				b.com2i + b.com3i + b.com4i + b.com5i + 
				b.com6i + b.com7i + b.com8i + b.com9i + 
				b.com10i + b.ind1i + b.ind2i + b.ind3i + 
				b.ind4i + b.ind5i + b.ind6i + b.agr1i + 
				b.rel1i + b.gov1i + b.gov2i + b.edu1i + b.edu2i) > 0;
			';
		end loop;
end$$;


-- check results
select *
FROM dgeo.hazus_blocks_to_census2010_blocks;
-- PERFECT MATCH!!!!!
