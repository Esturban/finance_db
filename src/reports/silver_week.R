
#Get silver prices this week
'https://finance.yahoo.com/quote/SI%3DF/history?p=SI%3DF' %>% read_html %>%
  html_nodes('#Col1-1-HistoricalDataTable-Proxy > section') %>% html_table() %>%
  .[[1]] -> si_raw

SI_Last <- si_raw[-nrow(si_raw),] %>% mutate(
  Date = as.Date(strptime(Date, "%b %d, %Y")),
  Open = as.numeric(gsub("\\,", "", Open)),
  High = as.numeric(gsub("\\,", "", High)),
  Low = as.numeric(gsub("\\,", "", Low)),
  Close = as.numeric(gsub("\\,", "", `Close*`)),
) %>%
  select(Date:Low,Close)%>%timetk::tk_xts(date_var = "Date",silent = T)

