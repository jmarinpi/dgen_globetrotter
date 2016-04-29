set role 'dgeo-writers';

-- add a new field
ALTER TABLE dgeo.bht_compilation
ADD g numeric;

UPDATE dgeo.bht_compilation a
set g = (t35km - temperaturefinal) / (3500 - depthfinal)
where depthfinal>= 300 and depthfinal <= 3250;