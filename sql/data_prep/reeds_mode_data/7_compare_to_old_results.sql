select a.pca_reg, a.tilt, a.azimuth,
	round(a.h01::NUMERIC, 2) as h01, round(b.h01::NUMERIC, 2) as h01,
	round(a.h02::NUMERIC, 2) as h02, round(b.h02::NUMERIC, 2) as h02,
	round(a.h03::NUMERIC, 2) as h03, round(b.h03::NUMERIC, 2) as h03,
	round(a.h04::NUMERIC, 2) as h04, round(b.h04::NUMERIC, 2) as h04,
	round(a.h05::NUMERIC, 2) as h05, round(b.h05::NUMERIC, 2) as h05,
	round(a.h06::NUMERIC, 2) as h06, round(b.h06::NUMERIC, 2) as h06,
	round(a.h07::NUMERIC, 2) as h07, round(b.h07::NUMERIC, 2) as h07,
	round(a.h08::NUMERIC, 2) as h08, round(b.h08::NUMERIC, 2) as h08,
	round(a.h09::NUMERIC, 2) as h09, round(b.h09::NUMERIC, 2) as h09,
	round(a.h10::NUMERIC, 2) as h10, round(b.h10::NUMERIC, 2) as h10,
	round(a.h11::NUMERIC, 2) as h11, round(b.h11::NUMERIC, 2) as h11,
	round(a.h12::NUMERIC, 2) as h12, round(b.h12::NUMERIC, 2) as h12,
	round(a.h13::NUMERIC, 2) as h13, round(b.h13::NUMERIC, 2) as h13,
	round(a.h14::NUMERIC, 2) as h14, round(b.h14::NUMERIC, 2) as h14,
	round(a.h15::NUMERIC, 2) as h15, round(b.h15::NUMERIC, 2) as h15,
	round(a.h16::NUMERIC, 2) as h16, round(b.h16::NUMERIC, 2) as h16,
	round(a.h17::NUMERIC, 2) as h17, round(b.h17::NUMERIC, 2) as h17

from diffusion_solar.solar_resource_by_pca_summary a
left join diffusion_solar.reeds_solar_resource_by_pca_summary_wide b
ON a.pca_reg = b.pca_reg
and a.tilt = b.tilt
and a.azimuth = b.azimuth
