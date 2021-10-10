price_bind<-function(from,to){
  
  if(!"xts"%in%class(from))stop("Not an XTS object")
  out<-rbind(OHLC(from),OHLC(to[paste0(max(index(from))+1,"::")]))
  out$r<-Delt(Cl(out),type = "log")
  out$r5<-na.omit(rollapply(out$r, 5,function(x) sum(x)))
  return(out)
}