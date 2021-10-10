# stats_table

wlog("Single Factor Modeling",
     head = T,
     level = 2)
wlog("===============================", level = 2)

wlog(
  'Estimating the single factor model coefficients and appending to dataset ...',
  level = 3
)
start <- Sys.time()
# colnames(ds_symbols)
ds_symbols %<>% dplyr::mutate(sfm_i= map2(.x = rt,
                                          .y=rt_i,
                                          function(.x,.y)
                                            tryCatch({
                                              .x %<>% dplyr::select(date, rt)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
                                              .y%<>%dplyr::select(date,rt_industry)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
                                              
                                              .x %>% left_join(.y,by='date')%>%tq_performance(
                                                Ra = rt,
                                                Rb=rt_industry,
                                                performance_fun = table.SFM,
                                                Rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
                                                scale               = as.numeric(TRADING_PERIODS)
                                              )
                                            }, error = function(err) {
                                              print(err)
                                              return(F)
                                            })))

if(SEC_RTS)ds_symbols %<>% dplyr::mutate(sfm_s= map2(.x = rt,
                                          .y=rt_s,
                                          function(.x,.y)
                                            tryCatch({
                                              .x %<>% dplyr::select(date, rt)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
                                              .y%<>%dplyr::select(date,rt_sector)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
                                              
                                              .x %>% left_join(.y,by='date')%>%tq_performance(
                                                Ra = rt,
                                                Rb=rt_sector,
                                                performance_fun = table.SFM,
                                                Rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
                                                scale               = as.numeric(TRADING_PERIODS)
                                              )
                                            }, error = function(err) {
                                              print(err)
                                              return(F)
                                            })))

end <- Sys.time()




if (dbExistsTable(localdb$con, c(SOURCE, 'rt_sfm'))) {
  wlog("Return statistics exist in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct symbol) from ',
        SOURCE,
        '.rt_sfm
        where fk_idr = ',
        ID_REF,' 
        and type = \'industry\'')
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time(),type='industry') %>%
    dplyr::select(symbol, type, fk_idr, last_update, sfm_i)%>%dplyr::filter(map_lgl(sfm_i,~is.data.frame(.))) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the single factor model coefficients for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_sfm'),
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
    dplyr::select(symbol,type, fk_idr, last_update,sfm_i)%>%dplyr::filter(map_lgl(sfm_i,~is.data.frame(.))) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_sfm'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}



if (SEC_RTS & dbExistsTable(localdb$con, c(SOURCE, 'rt_sfm'))) {
  wlog("Return statistics exist in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct symbol) from ',
        SOURCE,
        '.rt_sfm
        where fk_idr = ',
        ID_REF,' 
        and type = \'sector\'')
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time(),type='sector') %>%
    dplyr::select(symbol, type, fk_idr, last_update, sfm_s)%>%dplyr::filter(map_lgl(sfm_s,~is.data.frame(.))) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the single factor model coefficients for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_sfm'),
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
    dplyr::select(symbol,type, fk_idr, last_update,sfm_s)%>%dplyr::filter(map_lgl(sfm_s,~is.data.frame(.))) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_sfm'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}
# colnames(ds_symbols)<-c("symbol","stock_prices","company","industry","sector","industry_index","sector_index","rt","rt_i","rt_s","stats_pa","capm_industry","udr_industry","dist")
wlog('...completed estimating the single-factor model coefficients!', level = 3)
wlog('Total runtime: ', round(difftime(end, start, units = 'mins'), 2), 'minutes', level = 3)
#
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,stats_pa)%>%dplyr::filter(map_lgl(stats_pa,~is.data.frame(.)))%>%unnest()%>%arrange(desc(ArithmeticMean))
#
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,sfm_industry)%>%dplyr::filter(map_lgl(sfm_industry,~is.data.frame(.)))%>%unnest()#%>%arrange(desc(Excesskurtosis))
#
#
