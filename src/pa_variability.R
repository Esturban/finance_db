# variability_table

wlog("Stock Variability",
     head = T,
     level = 2)
wlog("===============================", level = 2)

wlog(
  'Calculating the stock variability ratios and appending to dataset ...',
  level = 3
)
start <- Sys.time()
# colnames(ds_symbols)
ds_symbols %<>% dplyr::mutate(variability = map(.x = rt,
                                                  # .y=rt_i,
                                                  # function(.x,.y)
                                                  function(.x)
                                                    tryCatch({
                                                      # print(str(.y))
                                                      .x %<>% dplyr::select(date, rt)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
                                                      # .y%<>%dplyr::select(date,rt_industry)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
                                                      .x %>% tq_performance(
                                                        Ra = rt,
                                                        Rb=NULL,
                                                        performance_fun = table.Variability,
                                                        # Rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
                                                        scale               = as.numeric(TRADING_PERIODS),
                                                        geometric=T
                                                      )
                                                      # .x%>%left_join(.y,by='date')%>%tq_performance(Ra = rt,
                                                      #                                               Rb=rt_industry,
                                                      #                                               performance_fun = table.Distributions#,
                                                      #                                               #rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
                                                      #                                               #scale               = as.numeric(TRADING_PERIODS)
                                                      #                                               )
                                                      #
                                                    }, error = function(err) {
                                                      print(err)
                                                      return(F)
                                                    })))

end <- Sys.time()



if (dbExistsTable(localdb$con, c(SOURCE, 'rt_variability'))) {
  wlog("Return statistics exist in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct symbol) from ',
        SOURCE,
        '.rt_variability
        where fk_idr = ',
        ID_REF)
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ds_symbols %>% dplyr::mutate(fk_idr = ID_REF, last_update = Sys.time()) %>%
    dplyr::select(symbol, fk_idr, last_update, variability) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the return statistics for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'rt_variability'),
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
    dplyr::select(symbol, fk_idr, last_update, variability) %>% unnest()
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'rt_variability'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}




wlog('...completed running variability statistics!', level = 3)
wlog('Total runtime: ', round(difftime(end, start, units = 'mins'), 2), 'minutes', level = 2)
#
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,variability_pa)%>%dplyr::filter(map_lgl(variability_pa,~is.data.frame(.)))%>%unnest()%>%arrange(desc(ArithmeticMean))
# #
# colnames(ds_symbols)
# as_tibble(ds_symbols)%>%dplyr::select(symbol,ipo.year,
#                                       company,
#                                       industry,sector,variability)%>%dplyr::filter(map_lgl(variability,~is.data.frame(.)))%>%unnest()%>%arrange(desc(MonthlyStdDev))
#
#
