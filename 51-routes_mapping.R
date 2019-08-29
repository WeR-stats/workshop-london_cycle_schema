##################################################
# LONDON Cycle Hire Schema - Data Visualizations #
##################################################


library('leaflet')
draw_segments <- function(sts, ste, data_path = file.path(Sys.getenv('PUB_PATH'), 'datasets', 'cycle_schemas', 'london', 'fst')){
    strSQL <- paste("
        SELECT sg.segment_id, x_lon1, y_lat1, x_lon2, y_lat2
        FROM routes rt
        	JOIN routes_segments rs ON rs.route_id = rt.route_id
        	JOIN segments sg ON sg.segment_id = rs.segment_id
        WHERE start_station_id =", sts, "AND end_station_id =", ste, "
        ORDER BY rs.id
    ")
    y <- dbm_do('cycle_hire_london', 'q', strSQL = strSQL)
    ys <- y %>% st_as_sf(coords = c('x_lon1', 'y_lat1'))
    ye <- y %>% st_as_sf(coords = c('x_lon2', 'y_lat2'))
    yt <- st_combine(cbind(ys, ye)) %>% st_cast('LINESTRING')
    st_crs(yt) <- 4326
    yb <- rgeos::gBoundary(as(yt, 'Spatial'))

    leaflet() %>%
        fitBounds(lng1 = yb@bbox[1], lng2 = yb@bbox[3], lat1 = yb@bbox[2], lat2 = yb@bbox[4]) %>% 
        addProviderTiles(providers$CartoDB.Positron, options = providerTileOptions(noWrap = TRUE)) %>%
        leafem::addFeatures(
            data = yt,
            color = 'blue',
            weight = 3
        ) %>% 
        addCircles(
            lng = y[1, x_lon1],
            lat = y[1, y_lat1],
            radius = 14,
            stroke = TRUE,
            weight = 1,
            fill = TRUE,
            fillColor = 'green'
        ) %>%
        addCircles(
            lng = y[nrow(y), x_lon2],
            lat = y[nrow(y), y_lat2],
            radius = 14,
            stroke = TRUE,
            weight = 1,
            color = 'black',
            fill = TRUE,
            fillColor = 'red'
        )
}


draw_segments(221, 212)

