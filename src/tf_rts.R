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

if(IND_RTS)ds_symbols %<>%
  dplyr::mutate(
  rt_i = map(.x = industry_index,  function(.x)
    tryCatch({
      getIndexReturns(index_tbl = .x,index_type = 'industry',
                      run = 10,
                      lags = ACF_LAG)->out
      colnames(out)<-dbSafeNames(colnames(out))
      return(out)
    }, 
    error = function(err) {
    wlog("Error code for industry",err,level=3)
    wlog("...to be removed.",level=3)
    return(F)
    })))

if(SEC_RTS)ds_symbols %<>%
  dplyr::mutate(  rt_s = map(.x = sector_index,  function(.x)
    tryCatch({
      getIndexReturns(index_tbl = .x,index_type = 'sector',
                      run = 10,
                      lags = ACF_LAG)->out
      colnames(out)<-dbSafeNames(colnames(out))
      return(out)
    }, error = function(err) {
      wlog("Error code for sector",err,level=3)
      wlog("...to be removed.",level=3)
      return(F)
    }))
)%>%as_tibble()
end<-Sys.time()


wlog('...completed data mutations!', level = 3)
wlog('Total runtime: ',round(difftime(end,start,units='mins'),2),'minutes', level = 3)

flush.console()
gc()
as_tibble(ds_symbols)%>%dplyr::select(symbol,ipo.year,industry,rt)%>%unnest()
