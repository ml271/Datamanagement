SP <- suppressPackageStartupMessages

SP(suppressWarnings(library(dplyr)))  # v 1.0.1 used for Datamanipulation 
SP(suppressWarnings(library(dbplyr))) # v 1.4.4 used for Datamanipulation 
SP(suppressWarnings(library(odbc)))   # v 1.2.3 used for Database connections
library(anytime)
library(lubridate)
#
# Setzen des Speicherortes für die berichte
#
b.path <- "W:/R/Datamanagement-2021data-edit/"
year <- 2021
#
# Connecting to Database
# Connection to the PRODUCTION Server
#
print("Server connection herstellen")
DBCONNECTION_ADLM <- DBI::dbConnect(odbc::odbc(),
                                          driver = "SQL Server",
                                          server="FVAFR-SD3v\\SQLLOGGER",
                                          database = "adlmx",
                                          uid = "sa",
                                          pwd = "meiernt#2015",
                                          TrustedConnection = TRUE,
                                          encoding = "latin1")
#
# Datenbank querys
#
# Db für die logger ID's
ADLM_DBO_CHANNELS <- dbGetQuery(
  DBCONNECTION_ADLM,
  paste("SELECT * FROM adlmx.dbo.channels"))


#ADLM_DBO_MEASUREMENTS <- dbGetQuery(
#  DBCONNECTION_ADLM,
#  paste("SELECT * FROM adlmx.dbo.measurements"))
#
#
# Standort ident
#
STANDORT_NAMES <- unique(ADLM_DBO_CHANNELS$location)
#
#
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

#
# Erzeugen des UNIX Timestamps von jetzt im gestern
# Problematik: Ein Automatisch mit der aktuellen Zeit erstellter Timestamp von gestern kann zur folge haben,
# dass ein messvorgang unvollständig erfasst wird.
# In der Annahme, dass zu einem bestimmten Zeitpunkt standardmässig kein upload geschieht wird die Zeit fest gesetzt und nur das Datum verändert
# Eventuell muss mit einem Min/Max datum gearbeitet werden um sicher zu stellen, dass keine Daten verloren gehen oder Doppelungen geschehen.
#
# Theorie bestätigt, funktioniert.
# 
TMP_TIME_MIN <- format((as.numeric(as.POSIXct(paste0(year, "-01-01 00:00:00"),tz = "UTC"))*1000),scientific=FALSE)
TMP_TIME_MAX <- format((as.numeric(as.POSIXct(paste0(year, "-12-31 23:59:59"),tz = "UTC"))*1000),scientific=FALSE)
# 
# Db query für den vorherigen Tag zwischen 00:00:00 und 23:59:59 über ALLE Logger
#
TMP_ADLM_DBO_MEASUREMENTS <- dbGetQuery(
  DBCONNECTION_ADLM,
  paste("SELECT * FROM adlmx.dbo.measurements where unixtime >= ",TMP_TIME_MIN,"and unixtime <= ", TMP_TIME_MAX))
#
#
# Schleife zum abarbeiten der einzelnen Standorte
for (x in 1:length(STANDORT_NAMES)) {
  # 
  # ueberpruefen ob der Standort in der Blacklist ist:
  # Wenn standort vorhanden ist wird überprüft ob er mit n gekennzeichnet ist und ob ein flächen Eintrag vorhanden ist
  # Wenn der Standort nicht in der Liste vorhanden ist wird davon ausgegangen, dass 
  # Der Filename der Station/Flächen name ist.
  #
  TMP_BLACKLIST <- filter(BLACKLIST, STANDORT_NAMES[x] == BLACKLIST$Eintrag_ADLM)
  if(TMP_BLACKLIST$Blacklist[1] == "n" && (TMP_BLACKLIST$Station_Flaeche[1] != "") == TRUE || nrow(TMP_BLACKLIST) == "0" ){
  #
  # 
  TMP_STANDORT_IDENT <- filter(ADLM_DBO_CHANNELS, ADLM_DBO_CHANNELS$location == STANDORT_NAMES[x])
  #
  TMP_MEASUREMENTS <- subset(TMP_ADLM_DBO_MEASUREMENTS, channelid %in% TMP_STANDORT_IDENT$id)
  #
  if(nrow(TMP_MEASUREMENTS) == "0"){
    next
  }
  #
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
    #
    # Speichern der Daten in Dateien
    # Hier wird unterschieden ob ein Namensschema in der BLACKLIST datei ist oder nicht
    #
    if(nrow(TMP_MEASUREMENTS_AUSGABE) > 0){
      #
      # Schreiben der Täglichen Import Dateien:
      if(nrow(TMP_BLACKLIST) > 0){
        #
        write.table(TMP_MEASUREMENTS_AUSGABE,file = paste(TMP_BLACKLIST$Station_Flaeche[1],"_ADLM",".csv",sep = ""), append = FALSE, quote = FALSE, sep = ",", col.names = TRUE, row.names = FALSE)
        TMP_MEASUREMENTS_AUSGABE = 0
        #
      } else {
        #
         write.table(TMP_MEASUREMENTS_AUSGABE,file = paste(TMP_MEASUREMENTS_AUSGABE$flaeche[1],"_ADLM",".csv",sep = ""), append = FALSE, quote = FALSE, sep = ",", col.names = TRUE, row.names = FALSE)
         TMP_MEASUREMENTS_AUSGABE = 0
      }
    }
  } else{
    next
  }
}


