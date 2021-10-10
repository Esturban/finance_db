data_transmute<-function(x,grp){
  
  x$val<- Cl(x)/as.numeric(first(Cl(x)))*100
  x$ind<-row(Cl(x))
  as.data.frame(coredata(x)[,c('ind','val','r5')])%>%dplyr::mutate(cat = grp)
  
}