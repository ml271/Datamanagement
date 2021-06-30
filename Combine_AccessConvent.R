library(lubridate)
library(dplyr)
library(RODBC)
library(xlsx)
library(gdata)
library(RODBC)
library(LoggerImports)
# int.Year <- 2020 # specify year of interesst

#try odbc connection to acces db

# path_db <- "O:/PROJEKT/CONVENT/LOGDATEN/DBDAT/Conventwald"
# dl_table <- "DL1_BTA1"
# with_flags <- FALSE
# 
# path_out <- "W:/R/Datamanagement-2021data-edit/data/"
# year <- 2021

source("functions/check_for_ts_gaps.R")
source("functions/deleteColumnsNa.R")
source("functions/write.fwf2.R")

######################################################################################

extract_Access_data <- function(path_db ="O:/PROJEKT/CONVENT/LOGDATEN/DBDAT/Conventwald", dl_table, year, with_flags, path_out, ExportLogger=T, long_data=T ){
  abbr.plot <- "CO"
  abbr.subplot <- substring(dl_table,1 ,3)
  ## open connection
  rodbc.connect <- odbcConnectAccess(path_db, DBMSencoding = "latin1")
  
  # get data from db.connection form target year
  SQLQuery_dl1 <- paste0(
    "SELECT * FROM ", dl_table, " WHERE Dat_Zeit BETWEEN #01/01/",year,"# AND #31/12/",year," 23:59:59#")
  dat <- sqlQuery(rodbc.connect, SQLQuery_dl1, stringsAsFactors = FALSE ,as.is=T)
  if (is.data.frame(dat)== FALSE)
  {
    cat(paste0("No data found in Access DB check dl.table is a vailid table in DB.
For Conventwald.mdb it has to be one of DL1_BTA1, DL2_BFI2, DL3_WFI2, DL4_WFI4, DL5_WFEN, DL6_BBU5 "))
    stop()
  }
  
  if (length(dat[,1]) == 0){
    cat(paste0("No data found in ", dl_table, " for target year: ", year))
    stop()
  }
  
  RODBC::odbcClose(rodbc.connect)
  dat <- dat %>%
    mutate(Datum = ymd_hms(Dat_Zeit)) %>% 
    arrange(Datum) %>% collect() %>% select(-Dat_Zeit) %>% select(Datum, everything())
   #checking for duplicated entries
  dup_check <- sum(duplicated(dat$Datum))
  if (dup_check != 0){
    dup_dat <- dat[duplicated(dat$Datum),]
  }
  #checking for data gaps
  gaps <- check_for_ts_gaps(dat$Datum, max_diff = 24*60)
  print(gaps)
  
  print(paste("Es wurden Daten vom" , range(dat$Datum, na.rm=T)[1], "bis zum", range(dat$Datum, na.rm=T)[2], "gefunden und zusammengefasst"))
  
  #delete columns iwth only Na values, and drop coulumns by names
  dat <- delt_col_only_na(dat) %>%  select( -any_of(c("cobunord","cobusost","cobuswes", "cofisued", "cofinord", "Proto_Dat", "Temp_898", "Temp_881", "Temp_899"))) %>%  select(-contains("Kanäle"))
 
  # get flag coulmns
  if( with_flags == FALSE){
    dat <- dat[,-grep("x_", names(dat))]
  }
  # delete names of flags columns
  names(dat)[str_detect(names(dat), "x_")] <- ""
  
  write.fwf2(dat, file = paste0(path_out, abbr.plot,"_", abbr.subplot, "_Access__", year,"_combine.dat"), year = year)
  print(paste( "Es wurde eine Logger-Combi-Datei datei für das Jahr", year, "erstellt und unter", path_out, "gespeichert."))
  
  ### CREATE R DataFrame for further use
  if (long_data == T){
    # prepare data in R-Format
    dat_long <- dat %>% pivot_longer(. , cols= -Datum,  names_to = "variable", values_to = "value") %>% 
      mutate(Plot = plot_name, SubPlot = subplot_name) %>% 
      select(Plot, SubPlot, Datum, variable, value)
    return(dat_long)
  }
  else(
    return(dat)
  )
}

#testing
#extract_Access_data(path_db = path_db, dl_table = "DL3_WFI4", path_out = path_out, year =year,with_flags = F )  
