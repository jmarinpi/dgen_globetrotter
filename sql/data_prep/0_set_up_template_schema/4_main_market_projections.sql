set role 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_main_inflation;
CREATE TABLE diffusion_template.input_main_inflation
(
	ann_inflation numeric NOT NULL
);

-- add more here...