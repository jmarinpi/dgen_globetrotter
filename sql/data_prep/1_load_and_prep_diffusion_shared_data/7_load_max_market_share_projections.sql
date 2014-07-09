﻿DROP TABLE IF EXISTS diffusion_shared.max_market_share;
CREATE TABLE diffusion_shared.max_market_share (
	years_to_payback integer,
	max_market_share_new numeric,
	max_market_share_retrofit numeric,
	sector text,
	source text);

SET ROLE 'server-superusers';
COPY diffusion_shared.max_market_share FROM '/srv/home/mgleason/data/dg_wind/MaxMarketShare_simplified.csv' with csv header;
RESET ROLE;

CREATE INDEX max_market_share_sector_btree ON diffusion_shared.max_market_share USING btree(sector);
CREATE INDEX max_market_share_source_btree ON diffusion_shared.max_market_share USING btree(source);
CREATE INDEX max_market_share_years_to_payback_btree ON diffusion_shared.max_market_share USING btree(years_to_payback);

VACUUM ANALYZE diffusion_shared.max_market_share;

-- rename to be consistent with input sheet
-- duplicate commercial and name industrial
-- change input sheet options tfor consistency