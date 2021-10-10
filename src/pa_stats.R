# stats_table

wlog("Summary Statistics", head = T, level = 2)
wlog("===============================", level = 2)

wlog('On returns, calculating statistics and appending to dataset ...',
     level = 3)
start <- Sys.time()
ds_symbols %<>% dplyr::mutate(stats = map(rt, function(.x)
  tryCatch({
    .x %>% tq_performance(Ra = rt, performance_fun = table.Stats)
    
  }, error = function(err)
    return(F))))

end <- Sys.time()

if (dbExistsTable(localdb$con, c(SOURCE, 'rt_stats'))) {
  wlog("Return statistics exist in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct symbol) from ',
        SOURCE,
        '.rt_stats
        where fk_idr = ',
        ID_REF
      )
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time()) %>%
    dplyr::select(symbol, fk_idr, last_update, stats) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))

    if(tbl_exists==0){
      
    wlog("Writing the return statistics for ",ID_REF,level=3)  
      
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_stats'),
      value = tbl_write,
      overwrite = F,
      append=T,
      row.names = F
    )
    wlog("...complete!",level=3)
  }
  
} else{
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time()) %>%
    dplyr::select(symbol, fk_idr, last_update, stats) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_stats'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}


wlog('...completed summary statistics!', level = 3)
wlog('Total runtime: ', round(difftime(end, start, units = 'mins'), 2), ' minutes', level = 3)
flush.console()
gc()