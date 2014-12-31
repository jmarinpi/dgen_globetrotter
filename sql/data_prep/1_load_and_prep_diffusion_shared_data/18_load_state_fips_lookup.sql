set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_shared.state_fips_lkup;
CREATE TABLE diffusion_shared.state_fips_lkup AS
select state_abbr, state_fips::integer
from esri.dtl_state_20110101;

-- add primary key
ALTER TABLE diffusion_shared.state_fips_lkup
ADD primary key (state_abbr);

-- add unique constraint
ALTER TABLE diffusion_shared.state_fips_lkup 
ADD CONSTRAINT state_fips_unique UNIQUE (state_fips);

select *
FROM diffusion_shared.state_fips_lkup 
order by state_fips;