sparks<-function(ohlc,period){
  spark_data <- data.frame(
    date = tail(index(ohlc),period),
    var1 = coredata(tail(Cl(ohlc),period)),
    var2 = coredata(tail(ohlc$r,period)),
    var3 = coredata(tail(ohlc$r5,period))
  )
  spark_1<-spark_box(
    data = spark_data[,c('date','Close')],
    title = scales::dollar(mean(spark_data$Close,na.rm=T)), 
    subtitle = paste0("Market Price (Avg Last ",period," Sessions)")
  )
  
  spark_2<-spark_box(
    data = spark_data[,c('date','r')],
    title = scales::percent(mean(spark_data$r,na.rm=T),accuracy = 0.1), 
    subtitle = paste0("Daily Return (Avg Last ",period," Sessions)")
  )
  
  spark_3<-spark_box(
    data = spark_data[,c('date','r5')],
    title = scales::percent(dplyr::last(spark_data$r5),accuracy = 0.1), 
    subtitle = paste0("Last 5 Day Cumulative Returns (%)")
  )
  
  
  div(
    style = "display: flex; flex-wrap: wrap; justify-content: center",
    div(spark_1, style = "width: 33%; border: solid;"),
    div(spark_2, style = "width: 33%; border: solid;"),
    div(spark_3, style = "width: 33%; border: solid;")
  )
  
}