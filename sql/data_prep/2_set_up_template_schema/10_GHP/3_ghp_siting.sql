set role 'diffusion-writers';


DROP TABLE IF EXISTs diffusion_template.input_ghp_siting_res_parcel_size CASCADE;
CREATE TABLE diffusion_template.input_ghp_siting_res_parcel_size
(
	sys_config TEXT not null,
	min_parcel_size_acres numeric not null
);

