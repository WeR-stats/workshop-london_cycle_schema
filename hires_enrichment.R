
pkgs <- c('data.table', 'fst')
lapply(pkgs, require, char = TRUE)
source('./helpers.R')
data_path <- file.path(Sys.getenv('PUB_PATH'), 'datasets', 'cycle_schemas', 'london')

hrs <- read_fst(file.path(data_path, 'fst', 'hires_start'), as.data.table = TRUE)
# hrs <- hrs[start_day >= '2018-01-01']

message('Adding year (YYYY)...')
hrs[, start_year := year(start_day)]
gc()

message('Adding quarter of year ([Q]x)...')
hrs[, start_qoy := factor(quarters(start_day), ordered = TRUE)]
gc()

message('Adding quarter (YYYY [Q]x)...')
ord_qrtrs <- sort( apply( 
    expand.grid(min(hrs$start_year, na.rm = TRUE):max(hrs$start_year, na.rm = TRUE), 1:4), 
    1, 
    function(x) paste(x, collapse = ' Q') 
))
hrs[, start_quarter := factor(paste(start_year, start_qoy), levels = ord_qrtrs, ordered = TRUE)]
gc()

message('Adding month of year (MMMM)...')
hrs[, start_moy := factor(months(start_day), levels = month.name, ordered = TRUE)]
gc()

message('Adding month (MM YYYY)...')
ord_months <- apply( expand.grid(month.abb, min(hrs$start_year, na.rm = TRUE):max(hrs$start_year, na.rm = TRUE)), 1, function(x) paste(x, collapse = ' ') )
hrs[, start_month := factor(paste(substr(start_moy, 1, 3), start_year), levels = ord_months, ordered = TRUE)]
gc()

message('Adding week of year (1:53)...')
hrs[, start_woy := lubridate::week(start_day)]
gc()

message('Adding week (YYYY-[W]01:52) and Monday date of week (YYYY-MM-DD)...')
y <- data.table(date = sort(unique(hrs$start_day)))
y[, week := factor(ISOweek::ISOweek(date), ordered = TRUE)][, dweek := ISOweek::ISOweek2date(paste0(week, '-1'))]
hrs <- y[hrs, on = c(date = 'start_day')]
setnames(hrs, c('date', 'week', 'dweek'), c('start_day', 'start_week', 'start_dweek'))
gc()

message('Adding day of week (DDDD) and day of month (1:31)...')
ord_days <- c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
hrs[, `:=`( start_dow = factor(weekdays(start_day), levels = ord_days, ordered = TRUE), start_dom = mday(start_day) )]
gc()

message('Adding daypart...')
dcuts <- data.table(
    cuts = c(-1, 5, 8, 11, 16, 20), 
    labels = c('Small hours (0-6)', 'Morning (6-9)', 'PreNoon (9-12)', 'AfterNoon (12-17)', 'Evening (17-21)', 'Night (21-24)')
)
hrs[, start_daypart := cut(start_hour, c(dcuts$cuts, Inf), labels = dcuts$labels, ordered = TRUE)]
gc()

message('Adding season...')
hrs[, season := 'winter']
hrs[start_month %in% c('March', 'April', 'May'), season := 'spring']
hrs[start_month %in% c('June', 'July', 'August'), season := 'summer']
hrs[start_month %in% c('September', 'October', 'November'), season := 'autumn']

message('Adding flag for working days and holidays...')
hrs[, is_workingday := 'Yes'][start_dow %in% c('Saturday', 'Sunday'), is_workingday := 'No']
gc()

message('Adding holidays to working days flag...')
# ???


# convert categorical into factors
cols <- c('season', 'is_workingday')
hrs[, (cols) := lapply(.SD, as.factor), .SDcols = cols]
gc()

message('Sorting... ')
setorderv(hrs, c('start_station_id', 'start_day', 'start_hour', 'start_min'))

message('Saving as fst with double ordering over start and end stations...')
write_fst_idx('hires', c('start_station_id', 'end_station_id'), hrs, file.path(data_path, 'fst'))

message('Saving as fst with double ordering over start station and month...')
write_fst_idx('hires', c('start_station_id', 'start_month'), hrs, file.path(data_path, 'fst'), fname = 'hires_smonth')

