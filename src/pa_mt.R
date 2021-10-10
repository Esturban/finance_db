# stats_table

wlog("Market Timing Models",head = T, level = 2)
wlog("===============================", level = 2)
# http://www.people.hbs.edu/rmerton/OnMarketTimingPart2.pdf
wlog('On returns, calculate the market timing models and appending to dataset ...', level = 3)
wlog('Warning: Multiple calculations', level = 3)
start <- Sys.time()
# colnames(ds_symbols)
ds_symbols %<>% dplyr::mutate(
  mt_hm_i = map2(.x=rt,.y=rt_i,function(.x,.y)tryCatch(
    {
      # print(str(.y))
      .x%<>%dplyr::select(date,rt)%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
      .y%<>%dplyr::select(date,rt_industry)%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
      .x%>%left_join(.y,by='date')%>%tq_performance(Ra = rt,
                                                    Rb=rt_industry,
                                                    performance_fun = MarketTiming,
                                                    Rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
                                                    # scale               = as.numeric(TRADING_PERIODS))
                                                    method               = "HM")
      
    },error=function(err){print(err);return(F)})),
  mt_tm_i = map2(.x=rt,.y=rt_i,function(.x,.y)tryCatch(
    {
      # print(str(.y))
      .x%<>%dplyr::select(date,rt)%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
      .y%<>%dplyr::select(date,rt_industry)%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
      .x%>%left_join(.y,by='date')%>%tq_performance(Ra = rt,
                                                    Rb=rt_industry,
                                                    performance_fun = MarketTiming,
                                                    Rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
                                                    # scale               = as.numeric(TRADING_PERIODS))
                                                    method               = "TM")
      
    },error=function(err){print(err);return(F)}))
  
  )

end<-Sys.time()
# colnames(ds_symbols)<-c("symbol","stock_prices","company","industry","sector","industry_index","sector_index","rt","rt_i","rt_s","stats_pa")
wlog('...completed market timing models!', level = 3)
wlog('Total runtime: ',round(difftime(end,start,units='mins'),2),'minutes', level = 3)
# 
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,stats_pa)%>%dplyr::filter(map_lgl(stats_pa,~is.data.frame(.)))%>%unnest()%>%arrange(desc(ArithmeticMean))

as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
                                      industry,sector,mt_hm_i,mt_tm_i)%>%dplyr::filter(map_lgl(mt_tm_i,~is.data.frame(.)))%>%unnest()%>%dplyr::select(-tort_industry)%>%arrange(desc(Beta))#%>%arrange(desc(ActivePremium))





