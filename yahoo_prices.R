



start <- Sys.time()
#Wtih writing, runtime: 11.66 mins

#Go through and load the stock data available from the symbols
wlog("Loading data...", head = T, level = 2)
# wlog("max(date)    -    Symbol",head=T,level=3)
# wlog("========================",level=3)
#
wlog("Getting the last observation ", level = 2)

last_date <-
  tbl(localdb, sql(
    paste0(
      'select symbol, max(date) as last_date from yahoo.prices where date >= \'',
      Sys.Date() - 30,
      '\' group by symbol'
    )
  )) %>% collect()



# dbWriteTable(localdb$con,c('yahoo','options'),hold_sample, row.names=FALSE,overwrite=T,append=F)

last_date_opt <-
  tbl(localdb, sql(
    paste0(
      'select symbol, max(expiry) as last_date_opt from yahoo.options group by symbol'
    )
  )) %>% collect()

all_exchanges %>% inner_join(last_date, by = 'symbol') %>% left_join(last_date_opt, by =
                                                                       'symbol') %>%
  # dplyr::filter(!is.na(last_date))%>%
  dplyr::mutate(
    #Get the stock data via tidyquant from yahoo
    stock_prices = map2(.x = symbol, .y = last_date, .f=function(.x, .y) {
      # wlog(paste0(as.POSIXct(.y),"       -     ",.x),level=3)
      price_data <- tq_get(x = .x, get = "stock.prices",
                           from = .y)
      
      return(price_data)
    }),
    #Get the associated dividends data via tidyquant from yahoo
    dividends = map2(
      .x = symbol,
      .y = last_date,
      .f = function(.x, .y) {
        tq_get(.x, get = "dividends", from = .y)
      }
    ),
    #Compute the daily log returns in using the quantmod method above
    
    #Get the options chains data via quantmod from yahoo
    options_chains = map2(
      .x = symbol,
      .y = last_date_opt,
      .f = function(.x, .y)
        tryCatch({
          if (!is.na(.y))
            getOptionChain(.x, NULL) %>% convertOption()
        }, error = function(err) {
          #print(err)
          0
        })
    ),
    #Get the associated splits data via tidyquant from yahoo
    splits = map2(
      .x = symbol,
      .y = last_date,
      .f = function(.x, .y) {
        tryCatch(tq_get(.x, get = "splits", from = .y),error=function(err)0)
      }
    ),
    last_update = as.POSIXct(Sys.time())
    
  ) %>% as_tibble() -> data_obj



end <- Sys.time()

wlog(paste0(
  "Total runtime for the query: ",
  difftime(end, start, units = "mins")
), level = 2)
end - start

wlog('Preparing data for the database', level = 3)

#ensuring that the names are db safe
colnames(data_obj) <- dbSafeNames(colnames(data_obj))
wlog('Creating the pricing data table', level = 4)

#parsing out the price data from the data object
data_obj %>% dplyr::select(symbol, last_update, stock_prices) %>% unnest(map(stock_prices,  ~
{
  as.data.frame(.)
})) %>% .[complete.cases(.$date), ] -> price_data

#parsing out the options data from the data object
wlog('Creating the options data table', level = 4)
tryCatch(
  data_obj %>% dplyr::select(symbol, last_update, options_chains) %>% unnest(map(options_chains,  ~
  {as.data.frame(.)->df_out
    if("expiry" %in% colnames(df_out))df_out<-df_out%>%.[complete.cases(.$expiry),]
    return(df_out)})) ,
  error = function(err)
    NULL
) -> options_data

#parsing out the dividends data from the data object
wlog('Creating the dividends data table', level = 4)
data_obj %>% dplyr::select(symbol, last_update, dividends) %>% unnest() %>%
  dplyr::select(-value) %>% .[complete.cases(.$dividends), ] -> dividends_data

#parsing out the splits data from the data object
wlog('Creating the splits data table', level = 4)
data_obj %>% dplyr::select(symbol, last_update, splits) %>% unnest(map(splits,  ~
{
  as.data.frame(.)
})) %>% dplyr::select(-`.`, -splits) %>% setNames(., c('symbol',
                                                       'last_update',
                                                       'date',
                                                       'splits')) %>%
  .[complete.cases(.$splits), ] -> splits_data

# wlog('Creating the financial reports data table',level = 4)
# data_obj%>%dplyr::select(symbol,last_update,ratios)%>%unnest(map(ratios,~{as.data.frame(.)}))%>%dplyr::select(-`.`)%>%unnest(map(data,~{as.data.frame(.)}))->report_data

start <- Sys.time()
# dbWriteTable(localdb$con,c('yahoo','prices'),price_data, row.names=FALSE,overwrite=T)
if (!is.null(price_data))
  dbWriteTable(
    localdb$con,
    c('yahoo', 'prices'),
    price_data,
    row.names = FALSE,
    overwrite = F,
    append = T
  )
# dbWriteTable(localdb$con,c('yahoo','prices'),price_data, row.names=FALSE)

# prices_out<-dbReadTable(con,c('yahoo','prices'),)
# dbRemoveTable(con,c('yahoo','prices'))
end <- Sys.time()
end - start

start <- Sys.time()
if (!is.null(options_data) & 'expiry'%in% colnames(options_data))
  dbWriteTable(
    localdb$con,
    c('yahoo', 'options'),
    options_data,
    row.names = FALSE,
    overwrite = F,
    append = T
  )
end <- Sys.time()
end - start

start <- Sys.time()
if (!is.null(dividends_data))
  dbWriteTable(
    localdb$con,
    c('yahoo', 'dividends'),
    dividends_data,
    row.names = FALSE,
    overwrite = F,
    append = T
  )
end <- Sys.time()
end - start

start <- Sys.time()
if (!is.null(splits_data))
  dbWriteTable(
    localdb$con,
    c('yahoo', 'splits'),
    splits_data,
    row.names = FALSE,
    overwrite = F,
    append = T
  )
end <- Sys.time()
end - start

# R version 3.5.1 (2018-07-02)
# Platform: x86_64-w64-mingw32/x64 (64-bit)
# Running under: Windows >= 8 x64 (build 9200)
#
# Matrix products: default
#
# locale:
#   [1] LC_COLLATE=English_Canada.1252  LC_CTYPE=English_Canada.1252    LC_MONETARY=English_Canada.1252
# [4] LC_NUMERIC=C                    LC_TIME=English_Canada.1252
#
# attached base packages:
#   [1] stats     graphics  grDevices utils     datasets  methods   base
#
# other attached packages:
#   [1] bindrcpp_0.2.2             RPostgres_1.1.1            RPostgreSQL_0.6-2          DBI_1.0.0.9001
# [5] tibbletime_0.1.1           finreportr_1.0.1           DSTrading_1.0              inline_0.3.15
# [9] IKTrading_1.0              roxygen2_6.1.1             digest_0.6.18              quantstrat_0.14.6
# [13] foreach_1.4.4              blotter_0.14.2             FinancialInstrument_1.3.1  Rcpp_0.12.18
# [17] tidyquant_0.5.5            forcats_0.3.0              stringr_1.3.1              readr_1.1.1
# [21] tidyr_0.8.1                tibble_1.4.2               ggplot2_3.0.0              tidyverse_1.2.1
# [25] quantmod_0.4-13            TTR_0.23-3                 PerformanceAnalytics_1.5.2 lubridate_1.7.4
# [29] Quandl_2.9.1               xts_0.11-1                 zoo_1.8-3                  purrr_0.2.5
# [33] dplyr_0.7.6                plyr_1.8.4                 RevoUtils_11.0.1           RevoUtilsMath_11.0.0
#
# loaded via a namespace (and not attached):
#   [1] httr_1.3.1       bit64_0.9-7      jsonlite_1.5     modelr_0.1.2     assertthat_0.2.0 blob_1.1.1       cellranger_1.1.0
# [8] yaml_2.2.0       globals_0.12.4   pillar_1.3.0     backports_1.1.2  lattice_0.20-35  glue_1.3.0       quadprog_1.5-5
# [15] rvest_0.3.2      colorspace_1.3-2 pkgconfig_2.0.2  broom_0.5.0      listenv_0.7.0    haven_1.1.2      scales_1.0.0
# [22] withr_2.1.2      lazyeval_0.2.1   cli_1.0.1        magrittr_1.5     crayon_1.3.4     readxl_1.1.0     fansi_0.2.3
# [29] future_1.10.0    nlme_3.1-137     MASS_7.3-50      xml2_1.2.0       tools_3.5.1      hms_0.4.2        munsell_0.5.0
# [36] compiler_3.5.1   rlang_0.2.1      grid_3.5.1       iterators_1.0.10 rstudioapi_0.8   rjson_0.2.20     timetk_0.1.1.1
# [43] boot_1.3-20      gtable_0.2.0     codetools_0.2-15 curl_3.2         R6_2.3.0         utf8_1.1.4       bit_1.1-14
# [50] bindr_0.1.1      commonmark_1.6   stringi_1.2.5    parallel_3.5.1   dbplyr_1.2.2     tidyselect_0.2.4
dbDisconnect(localdb$con)
