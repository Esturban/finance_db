# stats_table

wlog("Ârbitrary",
     head = T,
     level = 2)
wlog("===============================", level = 2)

wlog(
  'Calculating the stock drawdowns ratios and appending to dataset ...',
  level = 3
)
start <- Sys.time()
# colnames(ds_symbols)
ds_symbols %<>% dplyr::mutate(arb1 = map(.x = ad_rt,
                                                  # .y=rt_i,
                                                  # function(.x,.y)
                                                  function(.x)
                                                    tryCatch({
                                                      # print(str(.y))
                                                      .x %<>% dplyr::select(date, ad_rt)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
                                                      # .y%<>%dplyr::select(date,rt_industry)#%>%dplyr::filter(date>=Sys.Date()-CAPM_TF)
                                                      .x %>% tq_performance(
                                                        Ra = ad_rt,
                                                        Rb=NULL,
                                                        performance_fun = table.Arbitrary,
                                                        metrics=c(),
                                                        metricNames=c()
                                                        # Rf              = as.numeric(RFR) / as.numeric(TRADING_PERIODS),
                                                        # scale               = as.numeric(TRADING_PERIODS)
                                                      )
                                                      # .x%>%left_join(.y,by='date')%>%tq_performance(Ra = ad_rt,
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
# colnames(ds_symbols)<-c("symbol","stock_prices","company","industry","sector","industry_index","sector_index","ad_rt","rt_i","rt_s","stats_pa","capm_industry","udr_industry","dist")
wlog('...completed running drawdowns statistics!', level = 3)
wlog('Total runtime: ', round(difftime(end, start, units = 'mins'), 2), 'minutes', level = 3)
#
# as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
#                                       industry,sector,stats_pa)%>%dplyr::filter(map_lgl(stats_pa,~is.data.frame(.)))%>%unnest()%>%arrange(desc(ArithmeticMean))
#
as_tibble(ds_symbols)%>%dplyr::select(symbol,company,
                                      industry,sector,drawdowns_ratio)%>%dplyr::filter(map_lgl(drawdowns_ratio,~is.data.frame(.)))%>%unnest()#%>%arrange(desc(Excesskurtosis))
#
#
