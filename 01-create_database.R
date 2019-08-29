###############################################################
# LONDON Cycle Hire Schema - Create MySQL database and tables #
###############################################################

library(data.table)
source('helpers.R')

in_path <- './csv'

dbname <- 'cycle_hire_london'

create_db(dbname)

# BASE TABLE: stations ------------------------------------------------------------------------------------------------
x = "
	station_id SMALLINT(3) UNSIGNED NOT NULL COMMENT 'original from TFL',
    terminal_id CHAR(8) NULL DEFAULT NULL COMMENT 'original from TFL',
    x_lon DECIMAL(8,6) NULL DEFAULT NULL COMMENT 'original from TFL',
    y_lat DECIMAL(8,6) UNSIGNED NULL DEFAULT NULL COMMENT 'original from TFL',
    address VARCHAR(250) NULL DEFAULT NULL 
        COMMENT 'calculated from script <geocode_stations.R> using Google Maps API',
    postcode CHAR(7) NULL DEFAULT NULL 
        COMMENT 'calculated from script <geocode_stations.R> as the minimum distance postcode from given coordinates',
    OA CHAR(9) NULL DEFAULT NULL 
        COMMENT 'found using a join with geography_uk.postcodes',
    WPZ CHAR(9) NULL DEFAULT NULL 
        COMMENT 'found using a join with geography_uk.postcodes',
    place VARCHAR(35) NOT NULL DEFAULT '' COMMENT 'original from TFL',
    area VARCHAR(30) NOT NULL DEFAULT 'void' COMMENT 'original from TFL',
    docks TINYINT(2) UNSIGNED NULL DEFAULT NULL COMMENT 'updated once a day at midnight from script <update_data.R>',
    first_hire INT(8) UNSIGNED NULL DEFAULT NULL,
    last_hire INT(8) UNSIGNED NULL DEFAULT NULL,
    is_active TINYINT(1) UNSIGNED NOT NULL DEFAULT 0,
    hires_started MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' 
        COMMENT 'number of hires that started from the station towards ANY station',
    duration_started SMALLINT(5) UNSIGNED NULL DEFAULT NULL 
        COMMENT 'AVG duration (in seconds) for hires that started from the station towards ANY station',
    hires_ended MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' 
        COMMENT 'number of hires that ended in the station coming from ANY station',
    duration_ended SMALLINT(5) UNSIGNED NULL DEFAULT NULL 
        COMMENT 'AVG duration (in seconds) for hires that ended in the station coming from ANY station',
    hires_self MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' 
        COMMENT 'number of hires that started from and ended in the SAME station',
    duration_self SMALLINT(5) UNSIGNED NULL DEFAULT NULL 
        COMMENT 'AVG duration (in seconds) for hires that started from and ended in the SAME station',
    hires_started_noself MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' 
        COMMENT 'number of hires that started from the station towards ANOTHER station',
    duration_started_noself SMALLINT(5) UNSIGNED NULL DEFAULT NULL 
        COMMENT 'AVG duration (in seconds) for hires that started from the station towards ANOTHER station',
    hires_ended_noself MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' 
        COMMENT 'number of hires that ended in the station coming from ANOTHER station',
    duration_ended_noself SMALLINT(5) UNSIGNED NULL DEFAULT NULL 
        COMMENT 'AVG duration (in seconds) for hires that ended in the station coming from ANOTHER station',
    PRIMARY KEY (station_id),
    INDEX (terminal_id),
    INDEX (is_active),
    INDEX (postcode),
    INDEX (OA),
    INDEX (WPZ),
	INDEX (area),
    INDEX (first_hire) USING BTREE,
    INDEX (last_hire) USING BTREE
"
y <- fread(file.path(in_path, 'stations.csv'))
create_dbtable('stations', dbname, x, y)

# BASE TABLE: distances -----------------------------------------------------------------------------------------------
x = "
	start_station_id SMALLINT(3) UNSIGNED NOT NULL,
	end_station_id SMALLINT(3) UNSIGNED NOT NULL COMMENT 'id A < id B',
	distance SMALLINT(5) UNSIGNED NOT NULL COMMENT 'meters',
	time SMALLINT(5) UNSIGNED NOT NULL COMMENT 'seconds',
	hires SMALLINT(5) UNSIGNED NOT NULL DEFAULT '0',
	duration INT(8) UNSIGNED NULL DEFAULT NULL COMMENT 'average in seconds',
	PRIMARY KEY (start_station_id, end_station_id),
	INDEX (distance) USING BTREE,
	INDEX (time) USING BTREE,
	INDEX (hires) USING BTREE,
	INDEX (duration) USING BTREE
"
y <- fread(file.path(in_path, 'distances.csv'))
create_dbtable('distances', dbname, x, y)

# BASE TABLE: hires ---------------------------------------------------------------------------------------------------
x = "
	rental_id INT(10) UNSIGNED NOT NULL,
	bike_id SMALLINT(5) UNSIGNED NOT NULL,
	start_station_id SMALLINT(3) UNSIGNED NOT NULL,
	start_day DATE NOT NULL,
	start_hour TINYINT(2) UNSIGNED NOT NULL,
	start_min TINYINT(2) UNSIGNED NOT NULL,
	end_station_id SMALLINT(3) UNSIGNED NOT NULL,
	end_day DATE NOT NULL,
	end_hour TINYINT(2) UNSIGNED NOT NULL,
	end_min TINYINT(2) UNSIGNED NOT NULL,
	duration MEDIUMINT(6) UNSIGNED NOT NULL COMMENT 'seconds',
	PRIMARY KEY (rental_id),
	INDEX (bike_id),
	INDEX (start_station_id),
	INDEX (end_station_id),
	INDEX (start_day),
	INDEX (end_day),
	INDEX (start_hour),
	INDEX (end_hour)
"
create_dbtable('hires', dbname, x)

# BASE TABLE: current -------------------------------------------------------------------------------------------------
x = "
	updated_at INT(10) UNSIGNED NOT NULL,
	station_id SMALLINT(3) UNSIGNED NOT NULL,
	tot_docks TINYINT(3) UNSIGNED NOT NULL,
	free_docks TINYINT(3) UNSIGNED NOT NULL,
	bikes TINYINT(3) UNSIGNED NOT NULL,
	PRIMARY KEY (updated_at, station_id),
	INDEX updated_at (updated_at),
	INDEX station_id (station_id)
"
y <- fread(file.path(in_path, 'current.csv'))
create_dbtable('current', dbname, x, y)

# DIRECTIONS TABLE: routes --------------------------------------------------------------------------------------------
x = "
	route_id MEDIUMINT(6) UNSIGNED NOT NULL AUTO_INCREMENT,
	start_station_id SMALLINT(3) UNSIGNED NOT NULL,
	end_station_id SMALLINT(3) UNSIGNED NOT NULL COMMENT 'id A < id B',
	distance SMALLINT(5) UNSIGNED NOT NULL,
	time SMALLINT(5) UNSIGNED NOT NULL,
	hires SMALLINT(5) UNSIGNED NOT NULL DEFAULT '0',
	duration INT(8) UNSIGNED NULL DEFAULT NULL COMMENT 'average in seconds',
	has_route TINYINT(1) UNSIGNED NOT NULL DEFAULT '0',
	PRIMARY KEY (route_id),
	UNIQUE INDEX start_station_id_end_station_id (start_station_id, end_station_id),
	INDEX hires (hires) USING BTREE,
	INDEX duration (duration) USING BTREE,
	INDEX start_station_id (start_station_id),
	INDEX end_station_id (end_station_id),
	INDEX has_route (has_route)
"
y <- fread(file.path(in_path, 'routes.csv'))
create_dbtable('routes', dbname, x, y)

# DIRECTIONS TABLE: segments ------------------------------------------------------------------------------------------
x = "
	segment_id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	x_lon1 DECIMAL(8,6) NOT NULL,
	y_lat1 DECIMAL(8,6) UNSIGNED NOT NULL,
	x_lon2 DECIMAL(8,6) NOT NULL,
	y_lat2 DECIMAL(8,6) UNSIGNED NOT NULL,
	length SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'meters',
	duration SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'seconds',
	hires MEDIUMINT(8) UNSIGNED NULL DEFAULT NULL,
	PRIMARY KEY (segment_id),
	UNIQUE INDEX x_lon1_y_lat1_x_lon2_y_lat2 (x_lon1, y_lat1, x_lon2, y_lat2)
"
y <- fread(file.path(in_path, 'segments.csv'))
create_dbtable('segments', dbname, x, y)

# DIRECTIONS TABLE: routes_segments ---------------------------------------------------------------------------------
x = "
	id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	route_id MEDIUMINT(6) UNSIGNED NOT NULL,
	segment_id INT(10) UNSIGNED NOT NULL,
	PRIMARY KEY (id),
	INDEX route_id (route_id),
	INDEX segment_id (segment_id)
"
y <- fread(file.path(in_path, 'routes_segments.csv'))
create_dbtable('routes_segments', dbname, x, y)


# HELPER TABLE: calendar ----------------------------------------------------------------------------------------------
x = "
	datefield DATE NOT NULL,
    day_id TINYINT(1) UNSIGNED NOT NULL,
    day_txt CHAR(3) NOT NULL,
    day_txt_long CHAR(9) NOT NULL,
    is_weekday TINYINT(1) UNSIGNED NOT NULL DEFAULT '0',
    is_leap TINYINT(1) UNSIGNED NOT NULL DEFAULT '0',
    d0 INT(8) UNSIGNED NOT NULL COMMENT '20120104',
    d1 CHAR(6) NOT NULL COMMENT '04 Jan',
    d2 CHAR(8) NOT NULL COMMENT '04/01/12',
    d3 CHAR(8) NOT NULL COMMENT '04-01-12',
    d4 CHAR(9) NOT NULL COMMENT '04-Jan-12',
    d5 CHAR(9) NOT NULL COMMENT '04 Jan 12',
    d6 CHAR(11) NULL DEFAULT NULL COMMENT 'Wed, 04 Jan',
    d7 CHAR(15) NULL DEFAULT NULL COMMENT 'Wed, 04 Jan 12',
    day_of_month TINYINT(2) UNSIGNED NOT NULL,
	day_of_quarter TINYINT(2) UNSIGNED NULL DEFAULT NULL,
    day_of_year SMALLINT(3) UNSIGNED NOT NULL,
    day_of_quarters MEDIUMINT(7) UNSIGNED NULL DEFAULT NULL,
    day_of_years MEDIUMINT(7) UNSIGNED NOT NULL,
    day_last_month INT(8) UNSIGNED NULL DEFAULT NULL,
    day_last_year INT(8) UNSIGNED NULL DEFAULT NULL,
    days_past SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
    to_date TINYINT(2) UNSIGNED NOT NULL DEFAULT '0',
    w0 MEDIUMINT(6) UNSIGNED NOT NULL COMMENT '201201',
    w0d INT(8) UNSIGNED NULL DEFAULT NULL COMMENT '20120104',
    w1 CHAR(6) NULL DEFAULT NULL COMMENT '04 Jan',
    w2 CHAR(8) NULL DEFAULT NULL COMMENT '04/01/12',
    w3 CHAR(8) NULL DEFAULT NULL COMMENT '04-01-12',
    w4 CHAR(9) NULL DEFAULT NULL COMMENT '04-Jan-12',
    w5 CHAR(9) NULL DEFAULT NULL COMMENT '04 Jan 12',
    week_of_year TINYINT(2) UNSIGNED NULL DEFAULT NULL,
    week_last_year INT(6) UNSIGNED NULL DEFAULT NULL,
    last_week INT(6) UNSIGNED NULL DEFAULT NULL,
    weeks_past SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
    m0 MEDIUMINT(6) UNSIGNED NOT NULL COMMENT '201201',
    m1 CHAR(8) NULL DEFAULT NULL COMMENT 'Jan 12',
    m2 CHAR(8) NULL DEFAULT NULL COMMENT '01/12',
    m3 CHAR(9) NULL DEFAULT NULL COMMENT '01-12',
    month_of_year TINYINT(2) UNSIGNED NULL DEFAULT NULL,
    month_last_year MEDIUMINT(6) UNSIGNED NULL DEFAULT NULL,
    last_month MEDIUMINT(6) UNSIGNED NULL DEFAULT NULL,
    months_past SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
    q0 CHAR(6) NOT NULL COMMENT 'yyyyQx',
    qn SMALLINT(4) UNSIGNED NOT NULL COMMENT 'yyyyx',
    quarter_of_year CHAR(2) NULL DEFAULT NULL,
    quartern_of_year TINYINT(1) UNSIGNED NULL DEFAULT NULL,
    quarter_last_year CHAR(6) NULL DEFAULT NULL,
    quartern_last_year SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
    last_quarter SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
    y0 SMALLINT(4) UNSIGNED NOT NULL COMMENT 'yyyy',
    last_year SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
    PRIMARY KEY (datefield) USING BTREE,
    INDEX (d0) USING BTREE,
    INDEX (m0) USING BTREE,
    INDEX (q0) USING BTREE,
    INDEX (y0) USING BTREE,
    INDEX (qn),
    INDEX (w0) USING BTREE,
    INDEX (days_past) USING BTREE,
    INDEX (months_past) USING BTREE,
    INDEX (to_date) USING BTREE,
    INDEX (is_weekday),
    INDEX (weeks_past) USING BTREE,
    INDEX (day_id) USING BTREE,
    INDEX (day_of_year) USING BTREE,
    INDEX (day_of_years) USING BTREE,
    INDEX (day_of_quarter) USING BTREE,
    INDEX (day_of_quarters) USING BTREE,
    INDEX (day_of_month) USING BTREE,
    INDEX (day_last_month),
    INDEX (day_last_year),
    INDEX (week_of_year),
    INDEX (week_last_year),
    INDEX (month_of_year),
    INDEX (month_last_year),
    INDEX (quarter_of_year),
    INDEX (quarter_last_year),
    INDEX (quartern_of_year),
    INDEX (quartern_last_year),
    INDEX (is_leap),
    INDEX (last_week),
    INDEX (last_month),
    INDEX (last_quarter),
    INDEX (last_year)
"
create_dbtable('calendar', dbname, x)

# HELPER TABLE: areas -----------------------------------------------------------------------------------------------
x = "
	area_id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	name CHAR(30) NOT NULL COLLATE 'utf8_unicode_ci',
	PRIMARY KEY (area_id),
	INDEX name (name)
"
y <- fread(file.path(in_path, 'areas.csv'))
create_dbtable('areas', dbname, x, y)

# SUMMARY TABLE: last24 -----------------------------------------------------------------------------------------------
x = "
	updated_at INT(10) UNSIGNED NOT NULL,
	station_id SMALLINT(3) UNSIGNED NOT NULL,
	tot_docks TINYINT(3) UNSIGNED NOT NULL,
	free_docks TINYINT(3) UNSIGNED NOT NULL,
	bikes TINYINT(3) UNSIGNED NOT NULL,
	PRIMARY KEY (updated_at, station_id),
	INDEX updated_at (updated_at),
	INDEX station_id (station_id)
"
create_dbtable('last24', dbname, x)

# SUMMARY TABLE: smr -------------------------------------------------------------------------------------------------
x = "
	datetype TINYINT(2) UNSIGNED NOT NULL 
	    COMMENT '1- year, 2- quarter, 3-month, 4- week, 5- day, 6- hour, 8- to_date, 9- in_date',
	datefield INT(8) UNSIGNED NOT NULL,
	bikes SMALLINT(5) NOT NULL,
	hires INT(10) NOT NULL,
	duration SMALLINT(5) NOT NULL,
	PRIMARY KEY (datetype, datefield)
"
create_dbtable('smr', dbname, x)

# SUMMARY TABLE: smr_start --------------------------------------------------------------------------------------------
x = "
	datetype TINYINT(2) UNSIGNED NOT NULL 
	    COMMENT '1- year, 2- quarter, 3-month, 4- week, 5- day, 6- hour, 8- to_date, 9- in_date',
	station_id SMALLINT(3) UNSIGNED NOT NULL,
	datefield INT(8) UNSIGNED NOT NULL,
	bikes MEDIUMINT(6) NOT NULL,
	hires MEDIUMINT(6) NOT NULL,
	duration MEDIUMINT(6) NOT NULL,
	PRIMARY KEY (datetype, station_id, datefield)
"
create_dbtable('smr_start', dbname, x)

# SUMMARY TABLE: smr_end ----------------------------------------------------------------------------------------------
x = "
	datetype TINYINT(2) UNSIGNED NOT NULL 
	    COMMENT '1- year, 2- quarter, 3-month, 4- week, 5- day, 6- hour, 8- to_date, 9- in_date',
	station_id SMALLINT(3) UNSIGNED NOT NULL,
	datefield INT(8) UNSIGNED NOT NULL,
	bikes MEDIUMINT(6) NOT NULL,
	hires MEDIUMINT(6) NOT NULL,
	duration MEDIUMINT(6) NOT NULL,
	PRIMARY KEY (datetype, station_id, datefield)
"
create_dbtable('smr_end', dbname, x)

# SUMMARY TABLE: smr_start_end ----------------------------------------------------------------------------------------
x = "
	datetype TINYINT(2) UNSIGNED NOT NULL 
	    COMMENT '1-year, 2-quarter, 3-month, 4-week, 5-day, 6-hour, 8-to_date, 9-in_date, 11-, 12-, 13-',
	start_station_id SMALLINT(3) UNSIGNED NOT NULL,
	end_station_id SMALLINT(3) UNSIGNED NOT NULL,
	datefield INT(8) UNSIGNED NOT NULL,
	bikes MEDIUMINT(7) NOT NULL,
	hires MEDIUMINT(7) NOT NULL,
	duration MEDIUMINT(7) NOT NULL,
	PRIMARY KEY (datetype, start_station_id, end_station_id, datefield)
"
create_dbtable('smr_start_end', dbname, x)

# Clean & Exit --------------------------------------------------------------------------------------------------------
rm(list = ls())
gc()
