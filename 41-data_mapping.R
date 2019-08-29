###################################
# LONDON Cycle Hire Schema - Maps #
###################################

library(leaflet)
library(data.table)

sts <- fread('./csv/stations.csv')
sts <- sts[area != 'void']
bnd <- readRDS('./boundaries/MSOA')
dts <- sts[, .(duration = mean(duration_started)), MSOA]

bnd <- merge(bnd, dts, by.x = 'id', by.y = 'MSOA')
pal.poly <- colorQuantile("Blues", bnd$duration, n = 7)


leaflet() %>% 
    addProviderTiles(providers$CartoDB.Positron) %>% 
    fitBounds(min(sts$x_lon), min(sts$y_lat), max(sts$x_lon), max(sts$y_lat)) %>% 
    addPolygons(
        data = bnd,
        stroke = FALSE,
        color = 'grey',
        weight = 2,
        smoothFactor = 0.2,
        fill = TRUE,
        fillColor = ~pal.poly(bnd$duration),
        fillOpacity = 0.2,
        label = ~paste(id, round(duration/60, 2))
    ) %>%
    addCircleMarkers(
        data = sts,
        lng = ~x_lon, lat = ~y_lat,
        radius = 8,
        stroke = TRUE,
        color = 'black',
        weight = 1,
        fill = TRUE,
        fillColor = 'red',
        fillOpacity = 0.2,
        label = ~paste0(place, ', ', area)
    )
