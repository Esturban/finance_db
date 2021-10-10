#Database data loading
wlog("Price and Stock Data Assessment",head=T,level=2)
wlog("===============================",level=2)
start <- Sys.time()

wlog('loading companies...',level=3)
dbGetQuery(localdb$con, 'select distinct symbol, company, "ipo.year", industry, sector from yahoo.stocks')->companies
# companies
wlog(paste0('gathering from ', DATE_START,' to ',DATE_END,' of returns (or max date available) ...'),level=3)
dbGetQuery(
  localdb$con,
  paste0(
    'select distinct pr.* from yahoo.prices pr
    where date >= \'',
    DATE_START,
    '\'
    and symbol in (select distinct symbol from yahoo.stocks)'
  )
)%>%dplyr::select(-last_update)%>%as_tbl_time(index =
                                                date)%>%
  group_by(symbol)%>%
  nest(date:adjusted, .key = 'stock_prices')->price_recent

wlog("...DB data loaded for prices ! ",level=3)

price_data<-price_recent%>%inner_join(companies,by='symbol')
end<-Sys.time()
wlog('Total runtime: ', round(difftime(end, start, units = 'mins'), 2), ' minutes', level = 2)
flush.console()
gc()