-- find all of the residential points that are in boulder county
SELECT b.*
FROM wind_ds.county_geom a
LEFT JOIN wind_ds.pt_grid_us_res b
ON a.county_id = b.county_id
where a.state_abbr = 'CO'
and a.county = 'Boulder'

-- join all of the residential points in boulder county to the rates and iiijjjicf
    SELECT b.*, c.iiijjjicf, d.res_cents_per_kwh
    FROM wind_ds.county_geom a
    --join the points to the county
    LEFT JOIN wind_ds.pt_grid_us_res b
    ON a.county_id = b.county_id
    --join the iiijjjicf information to the points
    LEFT JOIN wind_ds.iiijjjicf_lookup c
    ON b.iiijjjicf_id = c.id
    --join the annual electric rate to the points
    LEFT JOIN wind_ds.annual_ave_elec_rates_2011 d
    ON b.annual_rate_gid = d.gid
    --isolate the results to only boulder county
    where a.state_abbr = 'CO'
    and a.county = 'Boulder';