-- for commercial, we isolated points from navteq that mapped to cbecs primary building activities
-- we won't do this for industrial because the navteq factypes are primarily if not exclusively commercial


-- isolate commercial points from hsip 2012 based on 2-digit naics codes (42 and up)
CREATE OR REPLACE VIEW hsip_2012.all_hsip_industrial_facilities AS
SELECT *
FROM hsip_2012.all_points_with_naics a
WHERE substring(a.naicscode_3 for 2)::integer < 42; -- this is based on NAICS 2-digit lookup (http://www.census.gov/cgi-bin/sssd/naics/naicsrch?chart=2012)

-- check for any null geoms -- none foudn
SELECT count(*)
FROM hsip_2012.all_hsip_industrial_facilities
where ST_GEometryType(the_geom_4326) is null;

-- extract daytime/nighttime pop ratio to points
DROP TABLE IF EXISTS dg_wind.hsip_industrial_pts_conus_pop_ratio_nightday;
CREATE TABLE dg_wind.hsip_industrial_pts_conus_pop_ratio_nightday (
	gid integer,
	table_number integer,
	pop_ratio_nightday float);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','hsip_2012.all_hsip_industrial_facilities','table_number',
			'SELECT a.gid, a.table_number, ST_Value(b.rast,a.the_geom_4326) as pop_ratio_daynight
			FROM  hsip_2012.all_hsip_industrial_facilities a
			INNER JOIN dg_wind.ls2012_popratio_nightday_us_100x100 b
			ON ST_Intersects(b.rast,a.the_geom_4326)
			WHERE ST_GeometryType(a.the_geom_4326) = ''ST_Point'';',
		'dg_wind.hsip_industrial_pts_conus_pop_ratio_nightday', 'a',16);

-- review results in R: use check_daynight_popratio_for_commercial_points.R, and in arcgis -- see where to set the threshold for comm facilities
--**

-- buffer the points 
CREATE TABLE dg_wind.hsip_industrial_facility_buffers AS
SELECT gid,
	CASE WHEN ST_GeometryType(the_geom_4326) = 'ST_Point' THEN ST_Buffer(the_geom_4326::geography, 90)::geometry
	else the_geom_4326
	END as the_geom_4326, table_name, table_number
FROM hsip_2012.all_hsip_industrial_facilities;
--2370335  rows

-- make sure all the data is polygon data now
SELECT distinct(ST_GeometryType(the_geom_4326))
FROM dg_wind.hsip_industrial_facility_buffers;

-- this is too big to export to shapefile all at once, so create three views that split the data into pieces:
SELECT table_number, count(*)
from dg_wind.hsip_industrial_facility_buffers
group by table_number
order by table_number;

DROP TABLE IF EXISTS dg_wind.est_ind_pt_count;
CREATE TABLE dg_wind.est_ind_pt_count (
	est_gid integer,
	total_industrial_sales_mwh numeric,
	ind_pt_count integer);

select parsel_2('dav-gis','dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip','gid',
	'SELECT a.gid, a.total_industrial_sales_mwh, count(b.*) as ind_pt_count
	FROM dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip a
	INNER JOIN dg_wind.hsip_industrial_facility_buffers b
	ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
	where a.total_industrial_sales_mwh > 0
	group by a.gid, a.total_industrial_sales_mwh
	order by ind_pt_count asc','dg_wind.est_ind_pt_count','a',16);

select *
FROM dg_wind.est_ind_pt_count
where ind_pt_count = 0

CREATE OR REPLACE VIEW dg_wind.hsip_industrial_facility_buffers_part1 AS
SELECT *
FROM dg_wind.hsip_industrial_facility_buffers
where table_number < 52;

CREATE OR REPLACE VIEW dg_wind.hsip_industrial_facility_buffers_part2 AS
SELECT *
FROM dg_wind.hsip_industrial_facility_buffers
where gid >= 52 and gid <142;

CREATE OR REPLACE VIEW dg_wind.hsip_industrial_facility_buffers_part3 AS
SELECT *
FROM dg_wind.hsip_industrial_facility_buffers
where gid >= 142;




-- export to shapefiles --> S:\mgleason\DG_Wind\Data\Analysis\commercial_land_mask\commercial_facility_polygons\hsip_and_navteq_commercial_facility_buffers_part1.shp, hsip_and_navteq_commercial_facility_buffers_part2.shp, hsip_and_navteq_commercial_facility_buffers_part3.shp
-- in arc, merge into a single feature class in a geodatatabase -->F:\data\mgleason\DG_Wind\Data\Analysis\commercial_land_mask\commercial_facility_polygons\commercial_facs.gdb\commercial_facilities_combined
-- convert the commercial facilities shapefile to boolean (presence of commercial = 1)

-- also in arc, threshold the nighttime/daytime pop ratio grids ( <=1.05 --> 1 (is commercial) ) for conus, us , and ak
-- combine the two boolean grids using boolean-or logic (if commercial in either, it is commercial)
-- resample the grid to 200m grid consistent with the AWS gcf grids

-- convert this grid to points, then load those points to postgres as commercial points
-- load this grid to postgres and use to resample load to county level

