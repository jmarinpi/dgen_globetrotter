library(rCharts)
library(rMaps)
source('/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/r/maps/map_functions.R', chdir = T)

######################################################
# tests
violent_crime$rand = rnorm(nrow(violent_crime), mean = 100, sd = 25)
vc_small = violent_crime[violent_crime$Year >= 1960 & violent_crime$Year <=1980,]


m1 = anim_choro_multi(vc_small, 'State', c('Crime','rand'), pals = list(Crime = 'Reds', rand = 'Blues'),
                      ncuts = list(Crime = 5, rand = 5), height = 400, width = 800, scope = 'usa', classification = 'equal',
                      legend = T, labels = T, slider_var = 'Year', slider_step = 1, map_title = 'My Map', slider_width= 300,
                      legend_titles = list(Crime = 'Crime (count)', rand = 'Random Variable'))
m1$save('/Users/mgleason/m1.html', cdn = T)

m2 = anim_choro_multi(vc_small, 'State', c('Crime'), pals = list(Crime = 'Reds'),
                      ncuts = list(Crime = 5), height = 400, width = 800, scope = 'usa', , classification = 'jenks',
                      legend = T, labels = T, slider_var = 'Year', slider_step = 1, map_title = 'My Map',
                      legend_titles = list(Crime = 'Crime (count)'))
m2$save('/Users/mgleason/m2.html', cdn = T)

vc2010 = violent_crime[violent_crime$Year == 2010,]
m3 = anim_choro_multi(vc2010, 'State', c('Crime','rand'), pals = list(Crime = 'Reds', rand = 'Blues'),
                      ncuts = list(Crime = 5, rand = 5), height = 400, width = 800, scope = 'usa', 
                      legend = T, labels = T, map_title = 'My Map', 
                      legend_titles = list(Crime = 'Crime (count)', rand = 'Random Variable'))
m3$save('/Users/mgleason/m3.html', cdn = T)

m4 = anim_choro_multi(vc2010, 'State', c('Crime'), pals = list(Crime = 'Reds'),
                      ncuts = list(Crime = 5), height = 400, width = 800, scope = 'usa', 
                      legend = T, labels = T, map_title = 'My Map',
                      legend_titles = list(Crime = 'Crime (count)'))
m4$save('/Users/mgleason/m4.html', cdn = T)



##### OTHER STUFF
# save to html
# m2$save('/Users/mgleason/d.html', cdn = T)
# m2$show(cdn = T)
# to include in markdown
# put:
# <iframe src="file:///Users/mgleason/d.html" name="map" height=400 width=800>
#   </iframe>
  # in the md file`