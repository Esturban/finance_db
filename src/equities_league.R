
#tidy quant exchanges
wlog("Equities League Table",head=T,level=2)
wlog("==================================",head=T,level=2)
wlog("Loading the exchange data...",level=2)
tq_exchange("NASDAQ") -> NASDAQ_
tq_exchange("AMEX") -> AMEX_
tq_exchange("NYSE") -> NYSE_
all_exchanges <- rbind(AMEX_, NASDAQ_, NYSE_)
all_exchanges <- all_exchanges[which(!is.na(all_exchanges[, 'industry'])), ]

wlog("Separating out the unique sectors",level=3)

#Identifying the unique available sectors
sectors <- all_exchanges%>%pull(sector)%>%unique(.)
wlog("Separating out the unique industries",level=3)
#Identifying the unique available industries
industries <- all_exchanges%>%pull(industry)%>%unique()
all_exchanges[which(all_exchanges[,'sector']==sectors[1]),]
#Create the new market cap variable to numerically weigh the different stocks according to the sector
wlog("Type casting market cap to numeric",level=3)
all_exchanges[,'nom.mkt.cap']<-all_exchanges[,'market.cap']
#Converting to a data frame for ease of data cleaning.
all_exchanges<-all_exchanges%>%as.data.frame(stringsAsFactors=F)

future::plan(future::multiprocess)
#Getting the numeric values of the market cap print
all_exchanges[,'nom.mkt.cap']<-gsub('[$]|M|B','',all_exchanges[,'nom.mkt.cap'])%>%as.numeric
all_exchanges[,'M']<-F
all_exchanges[grepl("M",all_exchanges[,'market.cap']),'M']<-T
all_exchanges[,'B']<-F
all_exchanges[grepl("B",all_exchanges[,'market.cap']),'B']<-T

#Add the numeric multiplier for the nominal market cap
all_exchanges[,'nom.mkt.cap']<-ifelse(all_exchanges[,'B'],all_exchanges[,'nom.mkt.cap']*1000000000,all_exchanges[,'nom.mkt.cap']*1000000)
all_exchanges<-all_exchanges[which(!is.na(all_exchanges[,'nom.mkt.cap'])),]

industry_wt<-c()
for(i in industries)industry_wt[[i]]<-all_exchanges[which(grepl(i,all_exchanges[,'industry'])),'nom.mkt.cap']%>%sum(na.rm=T)
wlog("Industry Weightings",head=T,level=1)
wlog("====================",level=3)
wts<-list()
for (i in industries)
{
  wlog(paste0("Industry: ",i),head=T,level=3)
  wlog("===================",level=3)
  all_exchanges%>%
    filter(industry==i)%>%
    select(symbol,nom.mkt.cap)%>%
    dplyr::mutate(mkt.wt=(nom.mkt.cap/industry_wt[[i]]))->wts[[i]]
  
  # wlog("===================",level=2)
  wlog(paste0("Total Stocks in Weighting: ",nrow(wts[[i]])),level=2)
  wlog("Complete!",level=2)
  }
wts<-dplyr::bind_rows(wts)

wts<-wts[which(!is.infinite(wts[,'mkt.wt'])),-2]


# summary(wts)

wlog("Join weights onto full dataframe",head=T,level=2)

all_exchanges<-left_join(all_exchanges,wts,by='symbol')
all_exchanges<-all_exchanges[which(!is.na(all_exchanges[,'mkt.wt'])),]
wlog("Calculating shares outstanding",head=T,level=2)
all_exchanges[,'shares.out']<-round(all_exchanges[,'nom.mkt.cap']/all_exchanges[,'last.sale.price'],digits = 1)
all_exchanges[,'last_update']<-Sys.time()
all_exchanges[,'fk_idr']<-ID_REF

# 
# query<-"select distinct symbol from yahoo.prices"
# p_symbols= tbl(localdb, sql(query))%>%collect()%>%unlist()

wlog("post weighting and price results to db ",head=T,level=1)

dbWriteTable(localdb$con,c('yahoo','stocks'),all_exchanges, row.names=FALSE,overwrite=F,append=T)

wlog("Stock Highlights posted to DB!",level=1)
rm(wts,NASDAQ_,AMEX_,NYSE_)
