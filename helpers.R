write_fst_idx <- function(tname, cname, dts = NA, out_path = './', fname = NA, dname = NA){
    if(!is.na(dname)) dts <- dbm_do(dname, 'r', tname)
    setorderv(dts, cname)
    if(length(cname) == 1){
        yx <- dts[, .N, get(cname)]
    } else {
        yx <- dts[, .N, .(get(cname[1]), get(cname[2]))]
    }
    setnames(yx, c(cname, 'N'))
    yx[, n2 := cumsum(N)][, n1 := shift(n2, 1L, type = 'lag') + 1][is.na(n1), n1 := 1]
    setcolorder(yx, c(cname, 'N', 'n1', 'n2'))
    if(!is.na(fname)) tname <- fname
    write_fst(yx, file.path(out_path, paste0(tname, '.idx')))
    write_fst(dts, file.path(out_path, tname))
}

read_fst_idx <- function(fname, ref){
    yx <- read_fst(paste0(fname, '.idx'), as.data.table = TRUE)
    if(length(ref) == 1){
        y <- yx[get(names(yx)[1]) == ref[1], .(n1 = min(n1), n2 = max(n2))]
    } else {
        y <- yx[get(names(yx)[1]) == ref[1] & get(names(yx)[2]) == ref[2], .(n1, n2)]
    }
    read_fst(fname, from = y$n1, to = y$n2, as.data.table = TRUE)
}

create_db <- function(x){
    dbc = dbConnect(MySQL(), group = 'dataOps')
    dbSendQuery(dbc, paste('DROP DATABASE IF EXISTS', x))
    dbSendQuery(dbc, paste('CREATE DATABASE', x))
    dbDisconnect(dbc)
}

create_dbtable <- function(tname, dname, tdef, dts = NULL){
    dbc = dbConnect(MySQL(), group = 'dataOps', dbname = dname)
    dbSendQuery(dbc, paste('DROP TABLE IF EXISTS', tname))
    strSQL <- paste(
        "CREATE TABLE", tname, "(", tdef,
        ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ROW_FORMAT=FIXED"
    )
    dbSendQuery(dbc, strSQL)
    if(!is.null(dts)) dbWriteTable(dbc, tname, dts, row.names = FALSE, append = TRUE)
    dbDisconnect(dbc)
}

dbm_do <- function(dname, action = 'r', tname = NA, dts = NULL, strSQL = NA, trunc = TRUE, drop = FALSE){
    db_check <- FALSE
    tryCatch({
            dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = dname)
            db_check <- TRUE
        }, error = function(err) {
    })
    if(db_check){
        if(action == 'r') tflag <- dbExistsTable(dbc, tname)
        dbDisconnect(dbc)
    } else {
        stop('Can not connect to the specified database!')
    }
    switch(action,
        'w' = {
            if(is.na(tname)) stop('The table name is missing!')
            if(is.null(dts)) stop('The dataset is missing!')
            dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = dname)
            if(trunc) dbSendQuery(dbc, paste("TRUNCATE TABLE", tname))
            if(drop) dbSendQuery(dbc, paste("DROP TABLE IF EXISTS", tname))
            dbWriteTable(dbc, tname, dts, row.names = FALSE, append = TRUE)
        },
        'r' = {
            if(is.na(tname)) stop('The table name is missing!')
            if(!tflag) stop('The specified table does not exists!')
            dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = dname)
            y <- data.table( dbReadTable(dbc, tname) )
        },
        'q' = {
            dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = dname)
            y <- data.table( dbGetQuery(dbc, strSQL) )
        },
        's' = {
            dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = dname)
            dbSendQuery(dbc, strSQL)
        },
        message('The required action is not currently implemented')
    )
    dbDisconnect(dbc)
    if(action %in% c('r', 'q')) return(y)
}
