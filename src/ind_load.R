# ind_load
wlog("Index and Benchmark Sourcing and Aggregation",level=2,head=T)
wlog("=============================================",level=2)
wlog("Creating the sector indexes...",level=3)
start<-Sys.time()
sec_benchmarks <- (lapply(sectors, function(x) {
wlog("Building index for the following sector: ",x,level=4)  
  out <- price_data %>% dplyr::filter(sector == x)
  out <-
    out %>% unnest() %>% group_by(sector, date) %>% dplyr::summarise(
      index_value = sum(adjusted * volume, na.rm = T) /  (1000000*n_distinct(symbol))
      # sector_stocks = n_distinct(symbol)
    ) %>%
    ungroup()
  # assign(dbSafeNames(x),
  #        out,
  #        envir = .GlobalEnv)
  return(out)
}) %>% bind_rows(.)->sec_out)%>%as_tbl_time(index='date')%>%group_by(sector)%>%nest(date:index_value,.key = 'sector_index')


# sector_stocks %>% as_tibble()

wlog("Creating the industry indexes...",head=T,level=3)


# industry_stocks <- (
(lapply(industries, function(x) {
  out <- price_data %>% dplyr::filter(industry == x)
  # print('test')
  tryCatch({out <-
    out %>% unnest() %>% group_by(industry, date) %>% dplyr::summarise(
      index_value = sum(adjusted * volume, na.rm = T) / (1000000*n_distinct(symbol)),
      # index = n_distinct(symbol),
      last_update = Sys.time()
    ) %>%
    ungroup() 
  # assign(dbSafeNames(x),
  #        out,
  #        envir = .GlobalEnv)
  wlog("Building index for the following industry: ",x,level=4)  
  return(out)},error=function(err){tibble(industry = x,
                                         date = as.Date('1970-01-01'),
                                         index_value = NA,
                                         # indu_stocks = 0,
                                         last_update = Sys.time());
    wlog("...Failed.",level=2)})
}) %>% bind_rows(.)->ind_out)%>%as_tbl_time(index='date')%>%group_by(industry)%>%nest(date:index_value,.key = 'industry_index')%>%dplyr::filter(map_lgl(industry_index,~is.data.frame(.)))->ind_benchmarks#)%>%as_tbl_time(index='date')%>%group_by(industry)%>%nest(date:index_value,.key = 'ind_benchmark')
# pgConnect()


if (dbExistsTable(localdb$con, c(SOURCE, 'bm_sector'))){
  wlog("Sector index data exists in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct fk_idr) from ',
        SOURCE,
        '.bm_sector'
      )
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    sec_out %>%as_tibble()%>% dplyr::mutate(fk_idr = ID_REF, 
                              last_update = Sys.time()) %>%
    dplyr::select( fk_idr,last_update, date, index_value)
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the daily sector benchmark values for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'bm_sector'),
      value = tbl_write,
      overwrite = F,
      append=T,
      row.names = F
    )
    wlog("...complete!",level=3)
  }
  
}else{
  tbl_write <-
    sec_out%>%as_tibble() %>% dplyr::mutate(fk_idr = ID_REF, 
                              last_update = Sys.time()) %>%
    dplyr::select( fk_idr, sector,last_update, date, index_value)   
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'bm_sector'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}


if (dbExistsTable(localdb$con, c(SOURCE, 'bm_industry'))){
  wlog("Industry index data exists in the ",SOURCE," schema",level=2)
  tryCatch(
    dbGetQuery(
      localdb$con,
      paste0(
        'select count(distinct fk_idr) from ',
        SOURCE,
        '.bm_industry'
      )
    ) %>% collect()%>%unlist(.),
    error = function(err)
      return(F)
  ) -> tbl_exists
  
  tbl_write <-
    ind_out%>%as_tibble() %>% dplyr::mutate(fk_idr = ID_REF, 
                                            last_update = Sys.time()) %>%
    dplyr::select( fk_idr, industry,last_update, date, index_value)
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  
  if(tbl_exists==0){
    
    wlog("Writing the daily industry benchmark values for ",ID_REF,level=3)  
    
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'bm_industry'),
      value = tbl_write,
      overwrite = F,
      append=T,
      row.names = F
    )
    wlog("...complete!",level=3)
  }
  
}else{
  tbl_write <-
    ind_out %>%as_tibble()%>% dplyr::mutate(fk_idr = ID_REF, 
                                            last_update = Sys.time()) %>%
    dplyr::select(fk_idr,industry,  last_update, date, index_value)
  colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
  dbWriteTable(
    localdb$con,
    c(SOURCE, 'bm_industry'),
    value = tbl_write,
    overwrite = T,
    append=F,
    row.names = F
  )
}

# dbWriteTable(localdb$con,c('yahoo','custom_bm_industries'),as_tibble(ind_out),row.names=F,append=T,overwrite=F)
# dbWriteTable(localdb$con,c('yahoo','custom_bm_sectors'),as_tibble(sec_out),row.names=F,append=T,overwrite=F)
end<-Sys.time()
wlog('Total runtime: ', round(difftime(end, start, units = 'mins'), 2), ' minutes', level = 2)
flush.console()
gc()