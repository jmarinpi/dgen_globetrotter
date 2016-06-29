set role 'diffusion-writers';


DROP TABLE IF EXISTs diffusion_template.input_du_performance_projections CASCADE;
CREATE TABLE diffusion_template.input_du_performance_projections
(
	year integer not null,
	peaking_boilers_pct_of_peak_demand numeric not null,
	max_acceptable_drawdown_pct_of_initial_capacity numeric not null,
	plant_lifetime_yrs numeric not null,
	hydro_expected_drawdown_pct_per_yr numeric not null,
	EGS_expected_drawdown_pct_per_yr numeric not null,
	res_enduse_efficiency_factor numeric not null,
	com_enduse_efficiency_factor numeric not null,
	ind_enduse_efficiency_factor numeric not null,
	CONSTRAINT input_du_performance_projections_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);



DROP TABLE IF EXISTs diffusion_template.input_du_egs_reservoir_factors;
CREATE TABLE diffusion_template.input_du_egs_reservoir_factors
(
	year integer not null,
	resource_recovery_factor numeric not null,
	area_per_wellset_sqkm numeric not null,
	wells_per_wellset integer not null,
	CONSTRAINT input_du_egs_reservoir_factors_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT,
	CONSTRAINT input_du_egs_resource_recovery_factor_check 
		CHECK (resource_recovery_factor > 0 and resource_recovery_factor <= 1),
	CONSTRAINT input_du_egs_area_per_wellset_sqkm_check 
		CHECK (area_per_wellset_sqkm > 0),
	CONSTRAINT input_du_egs_wells_per_wellset_check 
		CHECK (wells_per_wellset > 0)
);
