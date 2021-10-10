wlog("Filtering Data", head = T, level = 2)
wlog("===============================", level = 2)
wlog('Filtering the price data by primary screening measures', level = 3)
start <- Sys.time()
ds_symbols%<>%dplyr::filter(map_lgl(stock_prices,~volume > MIN_VOLUME))
ds_symbols%>%dplyr::mutate(mean_vol = map_dbl(.x=stock_prices,~.x%>%pull(volume)%>%mean(.)),
                           first_obs = as.Date(map_dbl(.x=stock_prices,~.x%>%pull(date)%>%min(.))),
                           last_obs = as.Date(map_dbl(.x=stock_prices,~.x%>%pull(date)%>%max(.))),
                           last_price = map_dbl(.x=stock_prices,~.x%>%tail(1)%>%pull(adjusted)))%>%#dplyr::select(symbol,last_obs)
  dplyr::filter( mean_vol> MIN_VOLUME,
                 last_obs>=Sys.Date()-14,
                 first_obs<=Sys.Date()-360,
                 last_price >= MIN_PRICE,
                 last_price <= MAX_PRICE)%>%dplyr::select(symbol,
                                                          company,
                                                          industry,
                                                          sector,
                                                          ipo.year,mean_vol,
                                                          last_price,
                                                          stats_pa)%>%unnest()%>%arrange(desc(GeometricMean))
colnames(ds_symbols)
end<-Sys.time()
wlog('...completed data filtering step!', level = 3)
wlog('Total runtime: ',round(difftime(end,start,units='mins'),2),'minutes', level = 3)
flush.console()
gc()