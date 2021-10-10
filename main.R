# find the source directory from which the Rscript is called:
sourceDirectory <- function() {
  cmdArgs <- commandArgs(trailingOnly = FALSE)
  needle <- "--file="
  match <- grep(needle, cmdArgs)
  if (length(match) > 0) {
    # Rscript
    return(dirname(normalizePath(sub(
      needle, "", cmdArgs[match]
    ), winslash = "/")))
  } else {
    # 'source'd via R console
    return(dirname(normalizePath(sys.frames()[[1]]$ofile, winslash = "/")))
  }
}

# set this as the working directory (unless fails i.e. during testing)
WD <- tryCatch(
  expr = {
    sourceDirectory()
  },
  error = function(err) {
    return(getwd())
  }
)
setwd(WD)

# control variables:
c_config_success <- FALSE
c_lib_and_db     <- FALSE
c_equities_l     <- FALSE
c_price_l        <- FALSE
c_optim          <- FALSE
c_send_report    <- FALSE

# CONFIG (source the config file and attach the functions/objects to the SP)
.config <- new.env(parent = emptyenv())
try(expr = {
  sys.source(file = '../_config/config.R', env = attach(.config, name = ".config"))
  rm(WD, sourceDirectory)
  # if (!grepl("evalencia", WD)) {
  #   Sys.setenv(JAVA_HOME = 'C:\\Program Files\\Java\\jdk1.8.0_191\\jre')# for 64-bit version
  # } else{
  #   Sys.setenv(JAVA_HOME = 'C:\\Program Files\\Java\\jdk1.8.0_181\\jre')
  # }
})


# source('C:/Users/Este/OneDrive/01_dataprojects/sendmail_proj/functions/email_header.R',echo = F)


if (c_config_success)
{
  pkgs <-
    c(
      'plyr',
      'dplyr',
      'purrr',
      'magrittr',
      'Quandl',
      'tidyquant',
      'finreportr',
      'IKTrading',
      'DSTrading',
      'finreportr',
      'lubridate',
      'tseries',
      'rvest',
      'stringr',
      'quantmod',
      'tidyverse',
      'tibbletime',
      'RPostgreSQL',
      'RPostgres'
    )
  
  tf_ <- as.character(Sys.Date() - 30)
  tf_ <- paste0(tf_, "::")
  
  invisible(suppressPackageStartupMessages(sapply(pkgs, require, character.only = T))) ->
    lib.out
  # pgConnect()
  source('../../sendmail_proj/functions/email_header.R', echo = F)
  c_lib_and_db <- ifelse(abs(sum(lib.out) - length(lib.out)), {
    tryCatch({
      install.packages(names(lib.out)[!lib.out], dependencies = T)
      return(T)
    }, error = function(err)
      F)
  }, T)
  source('../_config/tq_fns.R', echo = F)
  pgConnect()
  
}
#Variables
ROC_WINDOW_RANGE <-10
lagger<-10


QUANTILES <- 8:10 / 10
ACF_LAG <- 30
ACF_THRESH <- 0.15
CAPM_TF <- 182 #last 182 days (~6 months)
IND_RTS <- T
SEC_RTS <- T
SHORTRUN_WINDOW<-10
MIN_VOLUME<-500000
MIN_PRICE<-1
MAX_PRICE<-15
DATE_START<-'2018-01-02'
RT_TYPE<-'log'
RT_REF<-'adjusted'
SCALE_REF<-'daily'
# DATE_START<- '2018-01-01'
# Frequency(xts(1:10,order.by = Sys.Date()-0:9))
# periodicity(xts(1:3,order.by = Sys.Date()-0:2))->sev
# Frequency
DATE_END<-Sys.Date()
SOURCE<-'yahoo'
time1<-Sys.time()
stats_string<-c('stats','dist','vari','dd','sfm','sr','capm','hm','mt','mr','ud')

if (dbExistsTable(localdb$con, c(SOURCE, 'runtime_meta')))
  {dbGetQuery(localdb$con,paste0('select max(id) from ',tolower(SOURCE),'.runtime_meta'))%>%unlist()->last_id
# dbReadTable(localdb$con,c('yahoo','runtime_meta'))
runtime_meta<-data.frame(date=Sys.Date(),
                         runtime=Sys.time(),
                         start_date=DATE_START,
                         end_date=DATE_END,
                         source=SOURCE,
                         min_volume=MIN_VOLUME,
                         max_price=MAX_PRICE,
                         min_price=MIN_PRICE,
                         industry_rates=IND_RTS,
                         sector_rates=SEC_RTS,
                         return_type=RT_TYPE,
                         return_reference=RT_REF,
                         scale_reference=SCALE_REF,
                         capm_tf=ifelse('capm'%in%stats_string,CAPM_TF,NULL),
                         performance_filter=paste0(stats_string,collapse=";\n"),
                         id=last_id+1,
                         stringsAsFactors = F
                         )
}


if (c_lib_and_db)
{
  if (dbExistsTable(localdb$con, c(SOURCE, 'runtime_meta'))){
    wlog("Runtime metadata exists in the ",SOURCE," schema",level=2)
    tryCatch(
      dbGetQuery(
        localdb$con,
        paste0(
          'select count(distinct id) from ',
          SOURCE,
          '.runtime_meta'
        )
      ) %>% collect()%>%unlist(.),
      error = function(err)
        return(F)
    ) -> tbl_exists
    
    tbl_write <-
      runtime_meta %>% dplyr::mutate(id = last_id+1->>ID_REF), 
                                     last_update = Sys.time()) 
    colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
    
    if(tbl_exists==0){
      
      wlog("Writing the runtime metadata for ID: ",runtime_meta$id,level=3)  
      
      dbWriteTable(
        localdb$con,
        c(SOURCE, 'runtime_meta'),
        value = tbl_write,
        overwrite = F,
        append=T,
        row.names = F
      )
      wlog("...complete!",level=3)
    }
    
  }else{
    tbl_write <-
      runtime_meta %>% dplyr::mutate(id=(last_id+1->>ID_REF), 
                                     last_update = Sys.time()) 
    colnames(tbl_write) <- dbSafeNames(colnames(tbl_write))
    dbWriteTable(
      localdb$con,
      c(SOURCE, 'runtime_meta'),
      value = tbl_write,
      overwrite = T,
      append=F,
      row.names = F
    )
  }
  
  source('src/equities_league.R')
  c_equities_l <- T
}

if (c_equities_l)
{
  source('src/price_load.R')
  c_price_l <- T
}

if (c_price_l)
{
  source('src/ind_load.R')
  c_index_l <- T
}

source('src/aggregate_vals.R')

source('src/rts.R')
# source('src/tf_rts.R')
#returns statistics mutations
if('stats'%in%stats_string)source('src/pa_stats.R')
if('dist'%in%stats_string)source('src/pa_distributions.R')
if('vari'%in%stats_string)source('src/pa_variability.R')
if('dd'%in%stats_string)source('src/pa_drawdowns.R')
# ds_symbols%>%saveRDS(file='ds_1yr.RDS')
#industry benchmarks
#' @single-factor-model
if('sfm'%in%stats_string)source('src/pa_sfm.R')
#' @specific-risk
if('sr'%in%stats_string)source('src/pa_sr.R')
#' @capm-industry
if('capm'%in%stats_string)source('src/pa_capm.R')
ds_symbols%>%as_tibble(.)

#' @capm-industry
if('hm'%in%stats_string)source('src/pa_hm.R')
if('mt'%in%stats_string)source('src/pa_mt.R')
if('ud'%in%stats_string)source('src/pa_updown.R')


# saveRDS(object = ds_symbols,file = 'ds_full.RDS')
saveRDS(object = ds_symbols,file = 'ds_since2018.RDS')
flush.console()
gc()
time2<-Sys.time()
time2-time1
# c_index_l<-T



# pgConnect()
