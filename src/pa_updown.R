# stats_table

wlog("Capture Ratios: Industry Benchmarks",head = T, level = 2)
wlog("===============================", level = 2)
#https://www.cfapubs.org/doi/full/10.2469/br.v3.n1.14
wlog('Calculating the capture ratios of returns with custom industry benchmarks and appending to dataset ...', level = 3)
start <- Sys.time()
# colnames(ds_symbols)
ds_symbols %<>% dplyr::mutate(
  udr_industry = map2(.x=rt,.y=rt_i,function(.x,.y)tryCatch(
    {
      # print(str(.y))
      .x%<>%dplyr::select(date,rt)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
      .y%<>%dplyr::select(date,rt_industry)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
      .x%>%left_join(.y,by='date')%>%tq_performance(Ra = rt,
                                                    Rb=rt_industry,
                                                    performance_fun = table.UpDownRatios#,
                                                    #rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS), 
                                                    #scale               = as.numeric(TRADING_PERIODS)
                                                    )
      
    },error=function(err){print(err);return(F)})))

end<-Sys.time()
# colnames(ds_symbols)<-c("symbol","stock_prices","company","industry","sector","industry_index","sector_index","rt","rt_i","rt_s","stats_pa")
wlog('...completed industry capture ratios!', level = 3)
wlog('Total runtime: ',round(difftime(end,start,units='mins'),2),'minutes', level = 3)
# 
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,stats_pa)%>%dplyr::filter(map_lgl(stats_pa,~is.data.frame(.)))%>%unnest()%>%arrange(desc(ArithmeticMean))
# 
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,udr_industry)%>%dplyr::filter(map_lgl(udr_industry,~is.data.frame(.)))%>%unnest()#%>%arrange(desc(ActivePremium))
# # 
# # 



