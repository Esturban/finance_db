wlog("Daily Symbol and Index Returns", head = T, level = 2)
wlog("===============================", level = 2)

wlog('Preparing model dataset computing returns...', level = 3)
start <- Sys.time()

ds_symbols %<>%
# ds_symbols %>% dplyr::filter(symbol%in%c('AAPL','FB','NFLX','GOOG'))%>%
  dplyr::mutate(
  rt = map(.x = stock_prices, function(.x)
              tryCatch({
                Ra = get_log_returnAd(.x) 
                colnames(Ra)<-dbSafeNames(colnames(Ra))
                return(Ra)
                
                },
                error = function(err) {
                  print(err);
                  return(F)
                })))

if(IND_RTS){
  wlog("Running the Industry Index returns",level=3)
  ds_industries<-ind_benchmarks%>%
  dplyr::mutate(
  rt_i = map(.x = industry_index,  function(.x)
    tryCatch({
      # print(str(.x))
      getIndexReturns(index_tbl = .x,index_type = 'industry',
                      run = NULL,
                      lags = ACF_LAG)->out
      # print(out)
      colnames(out)<-dbSafeNames(colnames(out))
      return(out)
    }, 
    error = function(err) {
    wlog("Error code for industry",err,level=3)
    wlog("...to be removed.",level=3)
    return(F)
    })))
  wlog("Joining the Industry Index returns onto the primary table",level=3)
  
ds_symbols%<>%left_join(ds_industries[,c('industry','rt_i')],by='industry')

}

# as_tibble(ds_symbols)%>%dplyr::select(symbol,rt_i.x)%>%unnest()
# as_tibble(ds_symbols)%>%dplyr::select(symbol,rt_i)%>%unnest()

if(SEC_RTS){
  wlog("Running the Sector Index returns",level=3)
  
  ds_sectors<-sec_benchmarks%>%
  dplyr::mutate(  rt_s = map(.x = sector_index,  function(.x)
    tryCatch({
      getIndexReturns(index_tbl = .x,index_type = 'sector',
                      run = NULL,
                      lags = ACF_LAG)->out
      colnames(out)<-dbSafeNames(colnames(out))
      return(out)
    }, error = function(err) {
      wlog("Error code for sector",err,level=3)
      wlog("...to be removed.",level=3)
      return(F)
    }))
    
)%>%as_tibble()
  
  wlog("Joining the Sector Index returns onto the primary table",level=3)
  
ds_symbols%<>%left_join(ds_sectors[,c('sector','rt_s')],by='sector')

}
end<-Sys.time()
# ds_symbols$rt_s<-NULL

if (dbExistsTable(localdb$con, c(SOURCE, 'rt_daily'))){
  wlog("Return data exists in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct symbol) from ',
        SOURCE,
        '.rt_daily
        where fk_idr = ',
        ID_REF)
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF,
                                 last_update = Sys.time()) %>%
    dplyr::select(symbol, fk_idr, last_update, rt) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the daily returns for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_daily'),
      value = tbl_write,
      overwrite = F,
      append=T,
      row.names = F
    )
    wlog("...complete!",level=3)
  }
  
}else{
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, 
                                 last_update = Sys.time()) %>%
    dplyr::select(symbol, fk_idr, last_update,rt) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_daily'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}

if (dbExistsTable(localdb$con, c(SOURCE, 'rt_industry'))){
  wlog("Industry return data exists in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct industry) from ',
        SOURCE,
        '.rt_industry
        where fk_idr = ',
        ID_REF)
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_industries %>% dplyr::mutate(fk_idr = ID_REF, 
                                 last_update = Sys.time()) %>%
    dplyr::select(industry, fk_idr, last_update, rt_i) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the daily industry returns for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_industry'),
      value = tbl_write,
      overwrite = F,
      append=T,
      row.names = F
    )
    wlog("...complete!",level=3)
  }
  
}else{
  tbl_write <-
    ds_industries %>% dplyr::mutate(fk_idr = ID_REF, 
                                 last_update = Sys.time()) %>%
    dplyr::select(industry, fk_idr, last_update,rt_i) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_industry'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}

if (dbExistsTable(localdb$con, c(SOURCE, 'rt_sector'))){
  wlog("Sector return data exists in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct sector) from ',
        SOURCE,
        '.rt_sector
        where fk_idr = ',
        ID_REF)
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_sectors %>% dplyr::mutate(fk_idr = ID_REF, 
                                 last_update = Sys.time()) %>%
    dplyr::select(sector, fk_idr, last_update, rt_s) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the daily sector returns for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_sector'),
      value = tbl_write,
      overwrite = F,
      append=T,
      row.names = F
    )
    wlog("...complete!",level=3)
  }
  
}else{
  tbl_write <-
    ds_sectors %>% dplyr::mutate(fk_idr = ID_REF, 
                                 last_update = Sys.time()) %>%
    dplyr::select(sector, fk_idr, last_update,rt_s) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_sector'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}



wlog('...completed data mutations!', level = 3)
wlog('Total runtime: ',round(difftime(end,start,units='mins'),2),'minutes', level = 3)

rm(tbl_write)
flush.console()
gc()
# as_tibble(ds_symbols)%>%dplyr::select(symbol,ipo.year,industry,rt)%>%unnest()
