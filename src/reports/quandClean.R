
quandClean<-function (stemCode, start_date = NULL, end_date = NULL, verbose = FALSE,...) 
{
  require(Quandl)
  require(xts)
  # Quandl.api_key(Sys.getenv("QUANDL_KEY"))
  
  #If dates are null, 1000+ or - for timeseries
  if (is.null(start_date)) {
    start_date <- Sys.Date() - 365 * 1000
  }
  if (is.null(end_date)) {
    end_date <- Sys.Date() + 365 * 1000
  }
  
  frontCode <- paste0(stemCode, 1)
  backCode <- paste0(stemCode, 2)
  front <- Quandl(frontCode, type = "xts", start_date = start_date,end_date = end_date, ...)
  #Finding the open interest column
  interestColname <- colnames(front)[grep(pattern = "Interest", colnames(front))]
  #Finding the volume column
  
  `if`(grepl("ICE",stemCode),
       volColname <- colnames(front)[grep(pattern = "Volume", colnames(front))]%>%.[!grepl("EFP|EFS|Block",.)],
       volColname <- colnames(front)[grep(pattern = "Volume", colnames(front))])
  #Example of a series where this is important:
  # https://www.quandl.com/data/CHRIS/LIFFE_S1-EUROSWISS-Futures-Continuous-Contract-1-S1-Front-Month
  # Close does not exist above, so it should find and arrange t
  if ("Close" %in% colnames(front)) {
    front <- front[, c("Open", "High", "Low", "Close", volColname, 
                       interestColname)]
  }
  else {
    front <- front[, c("Open", "High", "Low", "Settle", volColname, 
                       interestColname)]
  }
  # print(head(front))
  colnames(front) <- c("O", "H", "L", "C", "V", "OI")
  back <- Quandl(backCode, type = "xts", start_date = start_date, 
                 end_date = end_date, ...)
  if ("Close" %in% colnames(back)) {
    back <- back[, c("Open", "High", "Low", "Close", volColname, 
                     interestColname)]
  }
  else {
    back <- back[, c("Open", "High", "Low", "Settle", volColname, 
                     interestColname)]
  }
  colnames(back) <- c("BO", "BH", "BL", "BS", "BV", "BI")
  both <- cbind(front, back)
  both$BI[is.na(both$BI)] <- -1
  both$OI[is.na(both$OI)] <- -1
  both$lagBI <- stats::lag(both$BI)
  both$lagOI <- stats::lag(both$OI)
  both$OI[both$OI == -1] <- both$lagOI[both$OI == -1]
  both$BI[both$BI == -1] <- both$lagBI[both$BI == -1]
  both$OIdiff <- both$OI - both$BI
  both$tracker <- NA
  both$tracker[both$OIdiff < 0] <- -1
  both$tracker <- stats::lag(both$tracker)
  both$tracker[both$OIdiff > 0] <- 1
  both$tracker <- na.locf(both$tracker)
  frontRelevant <- both[both$tracker == 1, c(1:6)]
  backRelevant <- both[both$tracker == -1, c(7:12)]
  colnames(frontRelevant) <- colnames(backRelevant) <- c("Open", 
                                                         "High", "Low", "Close", "Volume", "OI")
  relevant <- rbind(frontRelevant, backRelevant)
  relevant[relevant == 0] <- NA
  instrument <- gsub("CHRIS/", "", stemCode)
  relevant$Open[is.na(relevant$Open)] <- relevant$Close[(which(is.na(relevant$Open)) - 
                                                           1)]
  NAs <- which(is.na(relevant$Open) | is.na(relevant$High) | 
                 is.na(relevant$Low) | is.na(relevant$Close))
  # print(head(NAs))
  if (verbose) {
    if (verbose) {
      message(paste(instrument, "had", length(NAs), "incomplete days removed from data."))
    }
    print(relevant[NAs, ])
  }
  if (length(NAs) > 0) {
    relevant <- relevant[-NAs, ]
  }
  relevant$ATR <- ATR(HLC = HLC(relevant))$atr
  spikes <- which(abs((relevant$Close - xts::lag.xts(relevant$Close))/relevant$ATR) > 
                    5 & abs((relevant$Close - xts::lag.xts(relevant$Close, -1))/relevant$ATR) > 
                    5)
  
  if (verbose) {
    message(paste(instrument, "had", length(spikes), "spike days removed from data."))
    print(relevant[spikes, ])
  }
  if (length(spikes) > 0) {
    relevant <- relevant[-spikes, ]
  }
  intraDaySpikes <- which((relevant$High - relevant$Low)/relevant$ATR > 
                            10 | relevant$Open > relevant$High | relevant$Close > 
                            relevant$High | relevant$Open < relevant$Low | relevant$Close < 
                            relevant$Low)
  if (verbose) {
    message(paste(instrument, "had", length(intraDaySpikes), 
                  "intraday spikes removed from data."))
    print(relevant[intraDaySpikes, ])
  }
  if (length(intraDaySpikes) > 0) {
    relevant <- relevant[-intraDaySpikes, ]
  }
  relevant$ATR <- NULL
  out <- relevant
  return(out)
}
