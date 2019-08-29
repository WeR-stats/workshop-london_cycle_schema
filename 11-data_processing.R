##############################################
# LONDON Cycle Hire Schema - Data Processing #
##############################################
# This script process data downloaded from <http://cycling.data.tfl.gov.uk/> and saved in <in_path>

pkgs <- c('data.table', 'fasttime', 'fst', 'ISOweek', 'jsonlite', 'stringr')
lapply(pkgs, require, char = TRUE)
source('helpers.R')

dbname <- 'cycle_hire_london'
in_path <- file.path(pub_path, 'ext_data', 'cycle_schemas', 'london')
out_path <- file.path(pub_path, 'datasets', 'cycle_schemas', 'london')

years <- 2019
fstart <- 1

tot_records_processed <- 0
for(yr in years){
    
    ypath <- file.path(in_path, yr)
    fnames <- list.files(ypath, pattern = '*.csv', full.names = TRUE)
    records_processed <- 0
    
    message('=========================================================')
    for(fl in fstart:length(fnames)){
        message('Working on file ', fl, ' out of ', length(fnames) )
        
        message(' + Reading dataset...')
        dts <- fread(
                fnames[fl],
                select = c("Rental Id", "Duration", "Bike Id", "End Date", "EndStation Id", "Start Date", "StartStation Id"),
                col.names = c('rental_id', 'duration', 'bike_id', 'end_date', 'end_station_id', 'start_date', 'start_station_id')
        )
        
        message(' + Recoding dates...')

        # delete records with station ids == 0 (depot?) and station_id >= 900 (?)
        dts <- dts[end_station_id > 0 & end_station_id < 900 & start_station_id > 0 & start_station_id < 900]
        
        # some files have the year in two letter format 
        lyr <- nchar(word(dts$start_date[1]))
        yr.str <- paste0('%d/%m/%', ifelse(lyr == 10, 'Y', 'y'))
        
        # some files have seconds
        dts[, `:=`(start_date = substr(start_date, 1, lyr + 6), end_date = substr(end_date, 1, lyr + 6))]
        
        # split start and end date into numeric day (YYMMDD), hour (HH) and minute (MM)
        dts[, `:=`(
            start_day = as.Date(gsub(' .*$', '', start_date), yr.str),
            start_hour = as.numeric(gsub('.* (.+):.*', '\\1', start_date)),
            start_min = as.numeric(gsub('.*:(.*)$', '\\1', start_date)),
            end_day = as.Date(gsub(' .*$', '', end_date), yr.str),
            end_hour = as.numeric(gsub('.* (.+):.*', '\\1', end_date)),
            end_min = as.numeric(gsub('.*:(.*)$', '\\1', end_date))
        )][, `:=`( start_date = NULL, end_date = NULL)]
        
        # reorder columns
        setcolorder(dts, 
            c('rental_id', 'bike_id', 
              'start_station_id', 'start_day', 'start_hour', 'start_min', 
              'end_station_id', 'end_day', 'end_hour', 'end_min', 
              'duration'
        ))
        
        message(' + Saving records into MySQL table...')
        dbm_do(dbname, 'w', 'hires', dts, trunc = FALSE)

        message(
            ' + Processed ', nrow(dts), ' records for file ', fl, 
            ' (from ', min(dts$start_day), ' to ', max(dts$start_day), ')'
        )
        message('---------------------------------------------------------')
        
        records_processed <- records_processed + nrow(dts)
        tot_records_processed <- tot_records_processed + nrow(dts)
        
    }
    message('\nTotal records processed for the year ', yr,': ', records_processed)
    message('=========================================================')

}
message('\n\nTotal records processed: ', tot_records_processed)

message('DELETING RIDES FROM/TO "VOID" STATIONS...')
dbm_do(dbname, 's', strSQL = "
    DELETE h FROM hires h JOIN (
         SELECT station_id FROM stations WHERE area = 'void' 
    ) t ON t.station_id = h.start_station_id
")

message('DELETING RIDES SAME STATIONS WITH DURATION <= 60 seconds')
dbm_do(dbname, 's', strSQL = "DELETE FROM hires WHERE duration <= 60 AND start_station_id = end_station_id")

message('UPDATE CNT OF HIRES AND AVG DURATION IN <routes>') # AVG(CASE WHEN duration < 86400 THEN duration ELSE 86400 END)) to limit single hire duration to 24h
dbm_do(dbname, 's', strSQL = "
    UPDATE routes rt JOIN (
        SELECT start_station_id, end_station_id, count(*) AS c, ROUND(AVG(duration)) as d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id, end_station_id
    ) t ON t.start_station_id = rt.start_station_id AND t.end_station_id = rt.end_station_id 
    SET rt.hires = t.c, rt.duration = t.d
")
dbm_do(dbname, 's', strSQL = "UPDATE routes SET duration = NULL WHERE hires = 0")

message('UPDATE first_hire, last_hire IN <stations>')
dbm_do(dbname, 's', strSQL = "
    UPDATE stations st JOIN ( 
    	SELECT start_station_id, MIN(start_day) AS sd, MAX(start_day) AS ed 
    	FROM hires 
    	GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.first_hire = t.sd, st.last_hire = t.ed
")

message('UPDATE is_active IN <stations>')
dbm_do(dbname, 's', strSQL = "UPDATE stations SET is_active = 1")
dbm_do(dbname, 's', strSQL = "
    UPDATE stations 
    SET is_active = 0
    WHERE docks = 0 OR ISNULL(postcode) OR ISNULL(first_hire) OR first_hire = 0 OR last_hire < ( SELECT d0 FROM calendar WHERE days_past = 6 )
")

message('UPDATE CNT OF HIRES AND AVG DURATION FOR "SELF HIRES" IN <stations>') 
dbm_do(dbname, 's', strSQL = "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id = end_station_id
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_self = t.c, st.duration_self = t.d
")

message('UPDATE CNT OF HIRES AND AVG DURATION FOR ALL STARTING HIRES IN <stations>') 
dbm_do(dbname, 's', strSQL = "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_started = t.c, st.duration_started = t.d
")

message('UPDATE CNT OF HIRES AND AVG DURATION FOR ALL ENDING HIRES IN <stations>') 
dbm_do(dbname, 's', strSQL = "
    UPDATE stations st JOIN (
        SELECT end_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        GROUP BY end_station_id
    ) t ON t.end_station_id = st.station_id
    SET st.hires_ended = t.c, st.duration_ended = t.d
")

message('UPDATE CNT OF HIRES AND AVG DURATION FOR NOSELF STARTING HIRES IN <stations>') 
dbm_do(dbname, 's', strSQL = "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_started_noself = t.c, st.duration_started_noself = t.d
")

message('UPDATE CNT OF HIRES AND AVG DURATION FOR NOSELF ENDING HIRES IN <stations>') 
dbm_do(dbname, 's', strSQL = "
    UPDATE stations st JOIN (
        SELECT end_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY end_station_id
    ) t ON t.end_station_id = st.station_id
    SET st.hires_ended_noself = t.c, st.duration_ended_noself = t.d
")

# load STATIONS from database
dts <- dbm_do(dbname, 'r', 'stations')

# recode columns
cols <- c('postcode', 'OA', 'WPZ', 'area')
dts[, (cols) := lapply(.SD, factor), .SDcols = cols]
dts[, terminal_id := as.numeric(terminal_id)]

# save as fst
setorder(dts, station_id)
write_fst(dts, file.path(out_path, 'stations'))

# save ROUTES as fst with index both on start and end stations (don't need recoding)
write_fst_idx('routes', 'start_station_id', out_path = out_path, dname = 'cycle_hire_london')
write_fst_idx('routes', 'end_station_id', out_path = out_path, fname = 'routes_end', dname = 'cycle_hire_london')

# load HIRES from database
dts <- dbm_do(dbname, 'r', 'hires')
setorderv(dts, c('start_station_id', 'start_day', 'start_hour', 'start_min', 'end_station_id'))

# recode
dts[, `:=`( start_day = as.IDate(fastPOSIXct(start_day)), end_day = as.IDate(fastPOSIXct(end_day)) )]

# save hires as fst with index over start and end stations
write_fst_idx('hires', 'start_station_id', dts, out_path)
write_fst_idx('hires', 'end_station_id', dts, out_path, 'hires_end')

# add time related columns (for start_station only)
dts[, `:=`( 
    s_year = year(start_day), 
    s_qoy  = factor(quarters(start_day), ordered = TRUE),
    s_moy  = factor(months(start_day), levels = month.name, ordered = TRUE) 
)]
ord_qrtrs  <- sort(apply( expand.grid(min(dts$s_year):max(dts$s_year), 1:4), 1, function(x) paste(x, collapse = ' Q')))
ord_months <- apply( expand.grid(month.abb, min(dts$s_year):max(dts$s_year)), 1, function(x) paste(x, collapse = ' '))
ord_days   <- c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
hcuts <- data.table(
    cuts = c(-1, 5, 8, 11, 16, 20), 
    labels = c('Small hours (0-6)', 'Morning (6-9)', 'PreNoon (9-12)', 'AfterNoon (12-17)', 'Evening (17-21)', 'Night (21-24)')
)
dts[, `:=`(
    s_quarter = factor(paste(s_year, s_qoy), levels = ord_qrtrs, ordered = TRUE),
    s_month   = factor(paste(substr(s_moy, 1, 3), s_year), levels = ord_months, ordered = TRUE),
    s_woy     = lubridate::week(start_day),
    s_week    = factor(ISOweek(start_day), ordered = TRUE)
)][, `:=`(
    s_dweek   = ISOweek2date(paste0(s_week, '-1')),
    s_dow     = factor(weekdays(start_day), levels = ord_days, ordered = TRUE), 
    s_dom     = mday(start_day),
    s_daypart = cut(start_hour, c(hcuts$cuts, Inf), labels = hcuts$labels, ordered = TRUE)
)]

# reorder columns
setcolorder(dts, c(
    'rental_id', 'bike_id', 'duration', 'start_station_id', 'start_day', 's_year', 's_qoy', 's_quarter', 
    's_moy', 's_month', 's_woy', 's_week', 's_dweek', 's_dow', 's_dom', 's_daypart'
))

# save hires as fst with index over stations and months (for start_station only)
write_fst_idx('hires', c('start_station_id', 's_month'), dts, out_path, 'hires_months')

# clean and exit
rm(list = ls())
gc()
