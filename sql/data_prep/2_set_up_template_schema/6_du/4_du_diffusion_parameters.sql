SET ROLE 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_du_bass_params CASCADE;
CREATE TABLE diffusion_template.input_du_bass_params
(
	p numeric NOT NULL,
	q numeric NOT NULL,
	time_to_full_subscription integer NOT NULL
);
