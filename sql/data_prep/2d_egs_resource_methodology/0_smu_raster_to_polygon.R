library(raster)
library(rgdal)

g = raster('/Volumes/Staff/mgleason/dGeo/Data/Source_Data/SMU_2016_Temperature_at_depth/t35')

p = rasterToPolygons(g, fun=NULL, n=4, na.rm=TRUE, digits = 3, dissolve=FALSE)

writeOGR(p, '/Volumes/Staff/mgleason/dGeo/Data/Source_Data/SMU_2016_Temperature_at_depth/shapefile/t35km.shp', layer = 1, driver = 'ESRI Shapefile', verbose = T)

# next: load this to postgres (dgeo.smu_t35km_2016) using QGIS