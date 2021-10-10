wlog("Daily Symbol and Index Returns", head = T, level = 2)
wlog("===============================", level = 2)

wlog('Preparing model dataset computing returns...', level = 3)
start <- Sys.time()

roc_name<-paste0('ROC_',SHORTRUN_WINDOW)
ds_symbols %<>%
# ds_symbols %>% dplyr::filter(symbol%in%c('AAPL','FB','NFLX','GOOG'))%>%
  dplyr::mutate(
  roc = map(.x = rt, function(.x)
              tryCatch({
                Ra = rt %>%
                  tq_transmute(
                    select     = rt,
                    mutate_fun = rollapply,
                    # rollapply args
                    width      = SHORTRUN_WINDOW,
                    align      = "right",
                    by.column  = FALSE,
                    FUN        = sum,
                    # FUN args
                    na.rm      = TRUE,
                    col_rename = roc_name
                  ) 
                },
                error = function(err) {
                  print(err);
                  return(F)
                })))


if(length(ACF_LAG)>1 & nchar(ACF_LAG[2]) >0)ds_symbols %<>%
  # ds_symbols %>% dplyr::filter(symbol%in%c('AAPL','FB','NFLX','GOOG'))%>%
  dplyr::mutate(
    acf_lags = map(.x = rt, .y= ,function(.x)
      tryCatch({
        Ra = %>% tq_mutate(
            select = rt:rt_10dROC,
            mutate_fun = lag.xts,
            k          = 1:ACF_LAG,
            col_rename = c(paste0("rt_", 1:ACF_LAG), paste0("rt_10d_", 1:ACF_LAG))
          )
      },
      error = function(err) {
        print(err);
        return(F)
      })))





wlog('...completed data mutations!', level = 3)
wlog('Total runtime: ',round(difftime(end,start,units='mins'),2),'minutes', level = 3)

flush.console()
gc()

# data(FANG)
# FANG
# 
# FANG_annual_returns <- FANG %>%
#   group_by(symbol) %>%
#   tq_transmute(select     = adjusted, 
#                mutate_fun = periodReturn, 
#                period     = "yearly", 
#                type       = "arithmetic")
