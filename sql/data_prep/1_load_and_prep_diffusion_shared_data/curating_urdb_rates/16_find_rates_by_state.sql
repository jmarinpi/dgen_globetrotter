DROP TABLE IF EXISTS diffusion_shared.urdb_rates_by_state_res;
CREATE TABLE diffusion_shared.urdb_rates_by_state_res AS
SELECT a.state_abbr, a.urdb_rate_id
from diffusion_shared.curated_urdb_rates_lookup_pts_res a
group by a.state_abbr, a.urdb_rate_id;

DROP TABLE IF EXISTS diffusion_shared.urdb_rates_by_state_com;
CREATE TABLE diffusion_shared.urdb_rates_by_state_com AS
SELECT a.state_abbr, a.urdb_rate_id
from diffusion_shared.curated_urdb_rates_lookup_pts_com a
group by a.state_abbr, a.urdb_rate_id;

DROP TABLE IF EXISTS diffusion_shared.urdb_rates_by_state_ind;
CREATE TABLE diffusion_shared.urdb_rates_by_state_ind AS
SELECT a.state_abbr, a.urdb_rate_id
from diffusion_shared.curated_urdb_rates_lookup_pts_ind a
group by a.state_abbr, a.urdb_rate_id;

-- add primary keys to facilitate joins
alter table diffusion_shared.urdb_rates_by_state_ind
ADD primary key (state_abbr, urdb_rate_id);

alter table diffusion_shared.urdb_rates_by_state_res
ADD primary key (state_abbr, urdb_rate_id);

alter table diffusion_shared.urdb_rates_by_state_com
ADD primary key (state_abbr, urdb_rate_id);