

WITH all_bins AS
         (
             SELECT a.county_id, 
                     b.uid as orientation_uid,
                     b.weight::numeric
             FROM diffusion_solar.counties_to_model a
             LEFT JOIN diffusion_solar.solar_ds_rooftop_availability b
             ON a.state_abbr = b.state_abbr
             and b.sector = 'res'
             WHERE a.county_id in  (531,533)
        ),
        sampled_bins AS 
        (
            SELECT a.county_id, 
                    unnest(sample(array_agg(a.orientation_uid ORDER BY a.orientation_uid),10,1 * a.county_id,True, array_agg(a.weight ORDER BY a.orientation_uid))) as orientation_uid
            FROM all_bins a
            GROUP BY a.county_id
            ORDER BY a.county_id, orientation_uid
        ), 
        numbered_samples AS
        (
            SELECT a.county_id, a.orientation_uid,
                   ROW_NUMBER() OVER (PARTITION BY a.county_id ORDER BY a.county_id, a.orientation_uid) as bin_id 
            FROM sampled_bins a
        )
        SELECT  a.county_id, a.bin_id,
                    b.pct_shaded, b.tilt, b.azimuth, b.weight
        FROM numbered_samples a
        LEFT JOIN diffusion_solar.solar_ds_rooftop_availability b
        ON a.orientation_uid = b.uid;