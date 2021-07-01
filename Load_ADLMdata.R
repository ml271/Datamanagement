

## TODO: Das Automatische Erstelln von einer Xlsx-datei die im als zweite Zeile (nach header) die einheiten der spalte beinhaltet.

source("functions/check_for_ts_gaps.R")
suppressWarnings(library(dplyr))  # v 1.0.1 used for Datamanipulation 
suppressWarnings(library(dbplyr)) # v 1.4.4 used for Datamanipulation 
suppressWarnings(library(odbc))  # v 1.2.3 used for Database connections
library(anytime)
library(lubridate)
library(xlsx)
library(tidyr)

year <- 2021
plot <- "Altensteig"
subplot <- "Fichte"
ExportLogger = TRUE
long_data = TRUE
path_out <- "W:/R/Datamanagement-2021data-edit/data/"

load_ADLM_data <- function(year, plot_name, subplot_name, LoggerExport= T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/"){ 
  
  print(c(plot_name, subplot_name))
  
  abbr.plot <- substring(plot, 1,2)
  abbr.sub <- substring(subplot, 1,2)
  
  #connect to ADLM Database
  connect.adlm <- DBI::dbConnect(odbc::odbc(),
                                      driver = "SQL Server",
                                      server="FVAFR-SD3v\\SQLLOGGER",
                                      database = "adlmx",
                                      uid = "sa",
                                      pwd = "meiernt#2015",
                                      TrustedConnection = TRUE,
                                      encoding = "latin1")

  #
  # get adlm channels
  ADLM_DBO_CHANNELS <- dbGetQuery(
    connect.adlm,
    paste("SELECT * FROM adlmx.dbo.channels"))
  STANDORT_NAMES <- unique(ADLM_DBO_CHANNELS$location)
  
  BLACKLIST <- read.csv(
    paste("./BLACKLIST_StandorteADLM.csv", sep = ""),
    sep = ";",
    check.names = TRUE,
    header = TRUE,
    encoding = "UTF-8"
  ) 
  # Wenn die csv nicht geladen wird, darf das skript nicht weiter laufen!
  if(nrow(BLACKLIST) <= 0){
    quit()
  }
  #Make a list with avaible plots and their ADLM name
  WHITELIST <- BLACKLIST %>% filter(Blacklist == "n" ) %>%  select(Eintrag_ADLM, Station_Flaeche)
  
  #Extract ADLM name for target plot
  exp_sta_flae <- paste(plot_name, subplot_name, sep="_")
  adlm_sta_flae <- WHITELIST %>% filter(Station_Flaeche == exp_sta_flae) %>% select( Eintrag_ADLM) %>%  pull()
  
  TMP_STANDORT_IDENT <- filter(ADLM_DBO_CHANNELS, ADLM_DBO_CHANNELS$location == adlm_sta_flae)
  TMP_MEASUREMENTS <- subset(TMP_ADLM_DBO_MEASUREMENTS, channelid %in% TMP_STANDORT_IDENT$id)
  
  if(nrow(TMP_MEASUREMENTS) == "0"){
    next
  }
  # Leere Vectoren erzeugen
  TMP_FLAECHE = 1:nrow(TMP_MEASUREMENTS)  #ADLM_DBO_CHANNELS$location
  TMP_TIME = as.POSIXct(TMP_MEASUREMENTS$unixtime/1000, origin="1970-01-01",tz="UTC")
  TMP_SENSOR = 1:nrow(TMP_MEASUREMENTS)   #ADLM_DBO_CHANNELS$name
  TMP_UNIT = 1:nrow(TMP_MEASUREMENTS)     #ADLM_DBO_CHANNELS$unit
  #
  for (i in 1:nrow(TMP_STANDORT_IDENT)) {
    # Filtern welche Werte zu den ID's gehören
    TMP <- filter(TMP_STANDORT_IDENT, TMP_STANDORT_IDENT$id == TMP_MEASUREMENTS$channelid[i])
    TMP_FLAECHE[which(TMP_MEASUREMENTS$channelid == TMP_STANDORT_IDENT$id[i])] = TMP_STANDORT_IDENT$location[i]
    TMP_SENSOR[which(TMP_MEASUREMENTS$channelid == TMP_STANDORT_IDENT$id[i])] = TMP_STANDORT_IDENT$name[i]
    TMP_UNIT[which(TMP_MEASUREMENTS$channelid == TMP_STANDORT_IDENT$id[i])] = TMP_STANDORT_IDENT$unit[i]
  }
  #
  # Die Rohdaten sind hier bereits heraus gesucht jetzt geht es an das erweitern
  #
  TMP_MEASUREMENTS_AUSGABE <-data.frame(unixtime = 1:nrow(TMP_MEASUREMENTS),
                                        time = 1:nrow(TMP_MEASUREMENTS),
                                        id = 1:nrow(TMP_MEASUREMENTS),
                                        flaeche = 1:nrow(TMP_MEASUREMENTS),
                                        sensor = 1:nrow(TMP_MEASUREMENTS),
                                        einheit = 1:nrow(TMP_MEASUREMENTS),
                                        wert_num = 1:nrow(TMP_MEASUREMENTS)
                                        )
  #
  # Zusammenbau des DF
  TMP_MEASUREMENTS_AUSGABE$unixtime = TMP_MEASUREMENTS$unixtime
  TMP_MEASUREMENTS_AUSGABE$time = TMP_TIME
  TMP_MEASUREMENTS_AUSGABE$id = TMP_MEASUREMENTS$channelid
  TMP_MEASUREMENTS_AUSGABE$flaeche = TMP_FLAECHE
  TMP_MEASUREMENTS_AUSGABE$sensor = TMP_SENSOR
  TMP_MEASUREMENTS_AUSGABE$einheit = TMP_UNIT
  TMP_MEASUREMENTS_AUSGABE$wert_num = TMP_MEASUREMENTS$data
  #
  # Neu ordnen des DF anahnd der unixtime(Ist am sichersten die unixtime zu nutzen)
  TMP_MEASUREMENTS_AUSGABE <- TMP_MEASUREMENTS_AUSGABE[order(TMP_MEASUREMENTS_AUSGABE$unixtime),]
  
  # now this is a normal dataframe again
  dat <- TMP_MEASUREMENTS_AUSGABE %>% select(-c(unixtime, id, flaeche, einheit)) %>%
    mutate( plot = plot, subplot = subplot)
  
  dat <- dat %>%  pivot_wider(id_cols = time,names_from = sensor, values_from = wert_num ) %>%  rename(Datum = time)
  names(dat) <- gsub( " ", "_", names(dat)) 
  
  # 1. check duplicated entries
  dup_check <- sum(duplicated(dat$Datum))
  if (dup_check != 0){
    dup_dat <- dat[duplicated(dat$Datum),]
    # we could do possible mor with these dublicated entries
  }
  
  dat <- 
    dat %>% arrange(Datum) %>%
    distinct(Datum, .keep_all= T) %>%
    #filter(!duplicated(dat$Datum)) %>%
    filter(year(Datum) >= year) %>% 
    filter(year(Datum) < year+1)
  
  gaps <- check_for_ts_gaps(ts= dat$Datum, max_diff= 24*60, list =F)
  print(gaps)
  
  print(paste("Es wurden Daten vom" , range(dat$Datum, na.rm=T)[1], "bis zum", range(dat$Datum, na.rm=T)[2], "gefunden und zusammengefasst"))
  
  if (LoggerExport == T){
    # prepare data for export in Logger-format
    dat_exp <- dat #%>% 
      #mutate(Datum = format(dat$Datum, format = "%d.%m.%y %H:%M"))
   head <- 
      TMP_MEASUREMENTS_AUSGABE %>% select(sensor, einheit) %>% distinct(sensor, einheit) %>% pivot_wider(names_from = sensor, values_from = einheit) %>% mutate(Datum = "Zeit")
    names(head) <- gsub( " ", "_", names(head))
    
    head <- head[, names(dat_exp)]
    
    if(sum(names(head) != names(dat_exp)) != 0){
      quit()
    }
    # Export data
    # erstelle ein zusammengefasste tabelle f?r das jeweilige jahr
    # missing unit row! add row in xlsx data format, for now export as csv without units,cuz better to read into R!
    # wb = createWorkbook()
    # sheet = createSheet(wb, paste(plot, subplot, sep="_"))
    # #addDataFrame(head, row.names = F, sheet = sheet)
    # dat_exp2 <- data.frame(dat_exp)
    # addDataFrame(dat_exp, sheet = sheet, startRow = 3, row.names = F, col.names = F,  byrow= F)
    # addDataFrame(head, sheet = sheet, startRow = 1, row.names = F, col.names = T, byrow = F)
    # saveWorkbook(wb, file=paste0(path_out, abbr.plot, "_Level2",abbr.sub, "_ADLM__", year,"_combine.xlsx"), password = NULL)
    #
    # write.table(head, file=paste0(path_out, abbr.plot, "_Level2",abbr.sub, "_ADLM__", year,"_combine.csv"),quote = F, sep=";", dec=".", row.names= F, col.names = TRUE)
    # write.table(dat_exp, file=paste0(path_out, abbr.plot, "_Level2",abbr.sub, "_ADLM__", year,"_combine.csv"),quote = F,sep=";", dec=".", row.names= F, col.names = F, append=T)
    # 
    write.csv2(dat_exp, file=paste0(path_out, abbr.plot, "_Level2",abbr.sub, "_ADLM__", year,"_combine.csv"), quote = F, row.names = F)
    print(paste( "Es wurde eine Logger-Combi-Datei datei für das Jahr", year, "erstellt und unter", path_out, "gespeichert."))
  
  }
    
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
tmp <- load_ADLM_data(year=year, plot=plot, subplot=subplot, LoggerExport= T, long_data = T)
