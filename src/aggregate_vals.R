wlog("Aggregate Date Data", head = T, level = 2)
wlog("===============================", level = 2)

wlog('Combining prices with custom benchmarks...', level = 3)
start <- Sys.time()
price_data %>%
  inner_join((
    ind_benchmarks
  ),
  by = c('industry'))->ds_symbols 

if(SEC_RTS)ds_symbols%<>%
  left_join((
    sec_benchmarks 
  ),
  by = c('sector'))

# if(TIME_MUTATE)ds_symbols%<>%
#   dplyr::mutate(
#     stock_prices = map(
#       .x = stock_prices,
#       ~ .x %>% as_tbl_time(index = date) %>% unique(.) %>% timetk::tk_augment_timeseries_signature(.)
#     )
#   )
end<-Sys.time()
wlog('...completed data mutations!', level = 3)
wlog('Total runtime: ',round(difftime(end,start,units='mins'),2),'minutes', level = 2)
# rm(price_data,ind_benchmarks,sec_benchmarks)
flush.console()
gc()