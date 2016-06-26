set role 'diffusion-writers';


DROP TABLE IF EXISTs diffusion_template.input_ghp_siting_res_parcel_size CASCADE;
CREATE TABLE diffusion_template.input_ghp_siting_res_parcel_size
(
	sys_config TEXT not null,
	min_parcel_size_acres numeric not null
);


-- Create view for sys_config naming convention
DROP VIEW IF EXISTS diffusion_template.input_ghp_siting_res_parcel_size_sys_config;
CREATE VIEW diffusion_template.input_ghp_siting_res_parcel_size_sys_config AS (
	SELECT 'closed vertical'::text as sys_config, min_parcel_size_acres
	FROM diffusion_template.input_ghp_siting_res_parcel_size
	UNION ALL
	SELECT 'closed horizontal'::text as sys_config, min_parcel_size_acres
	FROM diffusion_template.input_ghp_siting_res_parcel_size
);
