set role 'dgeo-writers';

-- cap temperatures below zero
-- these occur in a few small pockets for the 500 and 1000 m depth intervals
-- i investiated them in Q, and generally wells nearby are in the range of 1 to 20 degrees C
-- so let's just set them all to 10 and 

UPDATE dgeo.egs_temp_at_depth_all_update
set t_500 = 10
where t_500 < 0;
-- 1537 rows

UPDATE dgeo.egs_temp_at_depth_all_update
set t_1000 = 10
where t_1000 < 0;
-- 78 rows