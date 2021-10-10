# stats_table

wlog("CAPM Model: last ",
     CAPM_TF,
     " days",
     head = T,
     level = 2)
wlog("===============================", level = 2)

wlog('On returns, calculate the CAPM and appending to dataset ...',
     level = 3)
start <- Sys.time()
# colnames(ds_symbols)
ds_symbols %<>% dplyr::mutate(capm_i = map2(.x = rt, .y = rt_i, function(.x, .y)
  tryCatch({
    # print(str(.y))
    .x %<>% dplyr::select(date, rt) %>% dplyr::filter(date >= Sys.Date() -
                                                        CAPM_TF)
    .y %<>% dplyr::select(date, rt_industry) %>% dplyr::filter(date >=
                                                                 Sys.Date() - CAPM_TF)
    .x %>% left_join(.y, by = 'date') %>% tq_performance(
      Ra = rt,
      Rb = rt_industry,
      performance_fun = table.CAPM,
      #rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
      scale               = as.numeric(TRADING_PERIODS)
    )
    
  }, error = function(err) {
    print(err)
    return(F)
  })))



if(SEC_RTS)ds_symbols %<>% dplyr::mutate(capm_s = map2(.x = rt, .y = rt_s, function(.x, .y)
    tryCatch({
      # print(str(.y))
      .x %<>% dplyr::select(date, rt) %>% dplyr::filter(date >= Sys.Date() -
                                                          CAPM_TF)
      .y %<>% dplyr::select(date, rt_sector) %>% dplyr::filter(date >=
                                                                 Sys.Date() - CAPM_TF)
      .x %>% left_join(.y, by = 'date') %>% tq_performance(
        Ra = rt,
        Rb = rt_sector,
        performance_fun = table.CAPM,
        #rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
        scale               = as.numeric(TRADING_PERIODS)
      )
      
    }, error = function(err) {
      print(err)
      return(F)
    })))



if (dbExistsTable(localdb$con, c(SOURCE, 'rt_capm'))) {
  wlog("CAPM estimates exist in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct symbol) from ',
        SOURCE,
        '.rt_capm
        where fk_idr = ',
        ID_REF,' 
        and type = \'industry\'')
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time(),type='industry') %>%
    dplyr::select(symbol, type, fk_idr, last_update, capm_i)%>%dplyr::filter(map_lgl(capm_i,~is.data.frame(.))) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the CAPM coefficients for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_capm'),
      value = tbl_write,
      overwrite = F,
      append=T,
      row.names = F
    )
    wlog("...complete!",level=3)
  }
  
} else{
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time(),type='industry') %>%
    dplyr::select(symbol,type, fk_idr, last_update,capm_i)%>%dplyr::filter(map_lgl(capm_i,~is.data.frame(.))) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_capm'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}



if (SEC_RTS & dbExistsTable(localdb$con, c(SOURCE, 'rt_capm'))) {
  wlog("CAPM estimates exist in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct symbol) from ',
        SOURCE,
        '.rt_capm
        where fk_idr = ',
        ID_REF,' 
        and type = \'sector\'')
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time(),type='sector') %>%
    dplyr::select(symbol, type, fk_idr, last_update, capm_s)%>%dplyr::filter(map_lgl(capm_s,~is.data.frame(.))) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the CAPM results for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_capm'),
      value = tbl_write,
      overwrite = F,
      append=T,
      row.names = F
    )
    wlog("...complete!",level=3)
  }
  
} else if(SEC_RTS){
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time(),type='sector') %>%
    dplyr::select(symbol,type, fk_idr, last_update,capm_s)%>%dplyr::filter(map_lgl(capm_s,~is.data.frame(.))) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_capm'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}



end <- Sys.time()
# colnames(ds_symbols)<-c("symbol","stock_prices","company","industry","sector","industry_index","sector_index","rt","rt_i","rt_s","stats_pa")
wlog('...completed industry CAPM models!', level = 3)
wlog('Total runtime: ', round(difftime(end, start, units = 'mins'), 2), 'minutes', level = 3)
#
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,stats_pa)%>%dplyr::filter(map_lgl(stats_pa,~is.data.frame(.)))%>%unnest()%>%arrange(desc(ArithmeticMean))
#
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,capm_industry)%>%dplyr::filter(map_lgl(capm_industry,~is.data.frame(.)))%>%unnest()%>%arrange(desc(ActivePremium))
