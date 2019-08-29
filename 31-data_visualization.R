##################################################
# LONDON Cycle Hire Schema - Data Visualizations #
##################################################

y <- read_fst_idx(file.path(data_path, 'fst', 'hires'), 1)

library(ggplot2)

yt <- y[, .N, .( start_daypart, start_dow)]

ggplot(yt, aes(start_daypart, start_dow, fill = N)) + 
    geom_tile() +
    geom_text(aes(label = N), color = 'white') + 
    scale_fill_gradientn(colors = c('#99d8c9','#66c2a4','#41ae76','#238b45','#006d2c','#00441b'))


library(dygraphs)
