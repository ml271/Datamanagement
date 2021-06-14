library(lubridate)
library(dplyr)
library(RODBC)
library(xlsx)
library(gdata)
library(RODBC)
library(data.table)

tick <- Sys.time()
Sys.setenv(TZ = "UTC")
setwd("O:/PROJEKT/NIEDER/MEßNETZ/")

################################### functions ##############################################
source("W:/Datenbank Level2/Scripts/functions/write.fwf2.R")
source("W:/Datenbank Level2/Scripts/functions/check_for_ts_gaps.R")

# delete all names from flag coloumns
del_names_flagscolm <- function(df.flag){

  nam <- names(df.flag)
  nam[grep("x", nam)] <- ""
  #nam[seq(3, length(nam)-2, by=2)] <- ""
  names(df.flag) <- nam
  return(df.flag)
}


# export each year of a dataframe seperatly
export_each_year <- function(df, name) {
  yrs <- unique(year(df$Dat_Zeit_utc))
  for (i in yrs){
    print(i); print(name)
    
    df_i  <- df %>% filter(Dat_Zeit_utc >= paste0(i, "-01-01") & Dat_Zeit_utc <= paste0(i, "-12-31 23:59:59")) %>% 
                    select_if(~sum(!is.na(.)) > 0) %>% 
                    select( -Dat_Zeit_utc)
    # delete column names form flag coloumns
    df_i2 <- del_names_flagscolm(df_i)
    write.fwf2(df_i2, file=paste0("W:/Datenbank Level2/Import DB/Access/", name, "_", i, ".dat"), year = i)
  }
}

##################################### ALTENSTEIG ############################################

## set odbc connection to acces db
path.db <- "ALTENSTEIG"
# open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
## Get data from db
SQLQuery_alt <- paste0(
  "SELECT * FROM LoggerTab_Altensteig")
df.alt <- sqlQuery(rodbc.connect, SQLQuery_alt, stringsAsFactors = FALSE ,as.is=T) %>%
    mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
    arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()
## Check Data
  # 1. check duplicated entries
  dup_check <- sum(duplicated(df.alt$Dat_Zeit_utc))
  # 2. check for missing data
  gaps_alt <- check_for_ts_gaps(ts= df.alt$Dat_Zeit, max_diff= 24*60 )
  class(gaps_alt$Dat_diff)
  as.difftime(gaps_alt$Dat_diff, format= "%H:%M")
  as.numeric(gaps_alt$Dat_diff, units = "days")
  format(gaps_alt$Dat_diff)
  # 3. check for right date-time format und tz
  tz(df.alt$Dat_Zeit_utc);  class(df.alt$Dat_Zeit_utc); range(df.alt$Dat_Zeit_utc)
  df.alt %>%  filter( Dat_Zeit_utc == "2001-10-22 00:00:00 UTC")
  sum(duplicated(df.alt$Dat_Zeit))
  # 4. check data consistency
  table(df.alt$AL_Kan?le)

            
## Data Manipulation
  # 1. extract double entries and remove them from data
  if (dup_check != 0){
    dup_alt <- df.alt[duplicated(df.alt$Dat_Zeit),]
  }
  df.alt2 <- df.alt %>%  filter(!duplicated(df.alt$Dat_Zeit_utc))

  # 2. drop all coloums containing only NA values and coloums containing non relevant information
  df.alt3 <- df.alt2[,which(unlist(lapply(df.alt2, function(x) !all(is.na(x)))))] 
    # %>%
    # select(-c(Proto_Dat, Dat_Zeit_utc))
  
  # 3. write new time coloum
  df.alt4 <- df.alt3 %>%  
    mutate("Dat_Zeit" = strftime(df.alt3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC"))
  

  
## Dataexport as .dat (txt) in Delta T Loggerformat
# write.fwf2(df.alt4, file="W:/Datenbank Level2/Import DB/ALT_Bruhberg_altendatencombi.dat", year= year(range(df.alt$Dat_Zeit_utc)[1]))

##################################### BLAUEN ################################################
  
## set odbc connection to acces db
path.db <- "Blauen"
# open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
  
## Get data from db
SQLQuery_blau <- paste0(
    "SELECT * FROM Starlogger")
df.blau <- sqlQuery(rodbc.connect, SQLQuery_blau, stringsAsFactors = FALSE ,as.is=T) %>%
    mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
    arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()
  
## Check Data
  # 1. check duplicated entries
  dup_check <- sum(duplicated(df.blau$Dat_Zeit_utc))
  # 2. check for missing data
  gaps_blau <- check_for_ts_gaps(ts= df.blau$Dat_Zeit, max_diff= 24*60 )
  class(gaps_blau$Dat_diff)
  as.difftime(gaps_blau$Dat_diff, format= "%H:%M")
  as.numeric(gaps_blau$Dat_diff, units = "days")
  format(gaps_blau$Dat_diff)
  # check for right date-time format und tz
  tz(df.blau$Dat_Zeit_utc);  class(df.blau$Dat_Zeit_utc); range(df.blau$Dat_Zeit_utc)
  df.blau %>%  filter( Dat_Zeit_utc == "2001-10-22 00:00:00 UTC")
  sum(duplicated(df.blau$Dat_Zeit))
  # # check data consistense
  table(df.blau$Kan?le)


## Data Manipulation
  # 1. check for double entries and remove them
  if (dup_check != 0){
    dup_blau <- df.blau[duplicated(df.blau$Dat_Zeit),]
  }
  df.blau2 <- df.blau %>%  filter(!duplicated(df.blau$Dat_Zeit_utc))

  # 2. drop all coloums containing only NA values and non relevant information
  df.blau3 <- df.blau2[,which(unlist(lapply(df.blau2, function(x) !all(is.na(x)))))] 
    # %>%
    # select(-c(Proto_Dat, Dat_Zeit_utc))

  # 3. write new time coloum
  df.blau4 <- df.blau3 %>%  
    mutate("Dat_Zeit" = strftime(df.blau3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC")) 

## Dataexport as .dat (txt) in Delta T Loggerformat
# write.fwf2(df.blau4, file="W:/Datenbank Level2/Import DB/Blauen_altendatencombi.dat", year= year(range(df.blau$Dat_Zeit_utc)[1]))

################################# MUDAU ################################

## set odbc connection to acces db
path.db <- "BUCHEN"
# open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
## Get data from db
SQLQuery_mud <- paste0(
  "SELECT * FROM Starlogger")
df.mud <- sqlQuery(rodbc.connect, SQLQuery_mud, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
  arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()

## Check Data
  # 1. check duplicated entries
  dup_check <- sum(duplicated(df.mud$Dat_Zeit_utc))
  # 2. check for missing data
  gaps_mud <- check_for_ts_gaps(ts= df.mud$Dat_Zeit, max_diff= 24*60 )
  class(gaps_mud2Dat_diff)
  as.difftime(gaps_mud$Dat_diff, format= "%H:%M")
  as.numeric(gaps_mud$Dat_diff, units = "days")
  format(gaps_mud$Dat_diff)
  # check for right date-time format und tz
  tz(df.mud$Dat_Zeit_utc);  class(df.mud$Dat_Zeit_utc); range(df.mud$Dat_Zeit_utc)
  df.mud %>%  filter( Dat_Zeit_utc == "2001-10-22 00:00:00 UTC")
  sum(duplicated(df.mud$Dat_Zeit))
  # # check data consistense
  table(df.mud$Kan?le)


## Data Manipulation
  # 1. check for double entries and remove them
  if (dup_check != 0){
    dup_mud <- df.mud[duplicated(df.mud$Dat_Zeit),]
  }
  df.mud2 <- df.mud %>%  filter(!duplicated(df.mud$Dat_Zeit_utc))
  
  # 2. drop all coloums containing only NA values and non relevant information
  df.mud3 <- df.mud2[,which(unlist(lapply(df.mud2, function(x) !all(is.na(x)))))] 
    # %>%
    # select(-c(Proto_Dat, Dat_Zeit_utc))
  
  # 3. write new time coloum
  df.mud4 <- df.mud3 %>%  
    mutate("Dat_Zeit" = strftime(df.mud3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC")) 
  
## Dataexport as .dat (txt) in Delta T Loggerformat
# write.fwf2(df.mud4, file="W:/Datenbank Level2/Import DB/Mudau_altendatencombi.dat", year= year(range(df.mud$Dat_Zeit_utc)[1]))
  
  
############################## ESSLINGEN #################################
  
path.db <- "ESSLINGEN"
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
## Get data from db
SQLQuery_och <- paste0(
  "SELECT * FROM FVA_LoggerTab_Esslingen")
df.ess <- sqlQuery(rodbc.connect, SQLQuery_och, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
  arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()


  ## Check Data
  # 1. check duplicated entries
  dup_check <- sum(duplicated(df.ess$Dat_Zeit_utc))
  # 2. check for missing data
  gaps_ess <- check_for_ts_gaps(ts= df.ess$Dat_Zeit, max_diff= 24*60 )
  class(gaps_ess$Dat_diff)
  as.difftime(gaps_ess$Dat_diff, format= "%H:%M")
  as.numeric(gaps_ess$Dat_diff, units = "days")
  format(gaps_ess$Dat_diff)
  # check for right date-time format und tz
  tz(df.ess$Dat_Zeit_utc);  class(df.ess$Dat_Zeit_utc); range(df.ess$Dat_Zeit_utc)
  df.ess %>%  filter( Dat_Zeit_utc == "2003-10-22 00:00:00 UTC")
  sum(duplicated(df.ess$Dat_Zeit))
  # # check data consistense
  table(df.ess$Kanäle)
  
  
  
  ## Data Manipulation
  # 1. check for double entries and remove them
  if (dup_check != 0){
    dup_ess <- df.ess[duplicated(df.ess$Dat_Zeit),]
  }
  df.ess2 <- df.ess %>%  filter(!duplicated(df.ess$Dat_Zeit_utc))
  
  # 2. drop all coloums containing only NA values and non relevant information
  df.ess3 <- df.ess2[,which(unlist(lapply(df.ess2, function(x) !all(is.na(x)))))] 
  # %>%
  # select(-c(Proto_Dat, Dat_Zeit_utc))
  
  # 3. write new time coloum
  df.ess4 <- df.ess3 %>%  
    mutate("Dat_Zeit" = strftime(df.ess3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC")) 
  
  ## Dataexport as .dat (txt) in Delta T Loggerformat
  # write.fwf2(df.ess4, file="W:/Datenbank Level2/Import DB/Rosenfeld_altendatencombi.dat", year= year(range(df.ess$Dat_Zeit_utc)[1]))
  


############################## OCHSENHAUSE D?NGERFL?CHEN #################

## set odbc connection to acces db
path.db <- "OCHSENHAUSEN"
# open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
## Get data from db
SQLQuery_och <- paste0(
  "SELECT * FROM LoggerTab_Ochsenhausen")
df.och <- sqlQuery(rodbc.connect, SQLQuery_och, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
  arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()



## Check Data
  # 1. check duplicated entries
  dup_check <- sum(duplicated(df.och$Dat_Zeit_utc))
  # 2. check for missing data
  gaps_och <- check_for_ts_gaps(ts= df.och$Dat_Zeit, max_diff= 24*60 )
  class(gaps_och$Dat_diff)
  as.difftime(gaps_och$Dat_diff, format= "%H:%M")
  as.numeric(gaps_och$Dat_diff, units = "days")
  format(gaps_och$Dat_diff)
  # check for right date-time format und tz
  tz(df.och$Dat_Zeit_utc);  class(df.och$Dat_Zeit_utc); range(df.och$Dat_Zeit_utc)
  df.och %>%  filter( Dat_Zeit_utc == "2001-10-22 00:00:00 UTC")
  sum(duplicated(df.och$Dat_Zeit))
  # # check data consistense
  table(df.och$OC_Kan?le)
  


## Data Manipulation
    # 1. check for double entries and remove them
    if (dup_check != 0){
      dup_och <- df.och[duplicated(df.och$Dat_Zeit),]
    }
    df.och2 <- df.och %>%  filter(!duplicated(df.och$Dat_Zeit_utc))
    
    # 2. drop all coloums containing only NA values and non relevant information
    df.och3 <- df.och2[,which(unlist(lapply(df.och2, function(x) !all(is.na(x)))))] 
      # %>%
      # select(-c(Proto_Dat, Dat_Zeit_utc))
    
    # 3. write new time coloum
    df.och4 <- df.och3 %>%  
      mutate("Dat_Zeit" = strftime(df.och3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC")) 
    
    
    
    range(df.och4$Dat_Zeit_utc[which(!is.na(df.och4$OCTs))])
    range(df.och4$Dat_Zeit_utc[which(!is.na(df.och4$OxTsKS81))])
    range(df.och4$Dat_Zeit_utc[which(!is.na(df.och4$OCTsKS21))])
## Dataexport as .dat (txt) in Delta T Loggerformat
# write.fwf2(df.och4, file="W:/Datenbank Level2/Import DB/Ochsenhausen_altendatencombi.dat", year= year(range(df.och$Dat_Zeit_utc)[1]))

########################## ROSENFELD #####################################

## set odbc connection to acces db
path.db <- "Rosenfeld"
# open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
## Get data from db
SQLQuery_rsf <- paste0(
  "SELECT * FROM Starlogger")
df.rsf <- sqlQuery(rodbc.connect, SQLQuery_rsf, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
  arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()
## Check Data
  # 1. check duplicated entries
  dup_check <- sum(duplicated(df.rsf$Dat_Zeit_utc))
  # 2. check for missing data
  gaps_rsf <- check_for_ts_gaps(ts= df.rsf$Dat_Zeit, max_diff= 24*60 )
  class(gaps_rsf$Dat_diff)
  as.difftime(gaps_rsf$Dat_diff, format= "%H:%M")
  as.numeric(gaps_rsf$Dat_diff, units = "days")
  format(gaps_rsf$Dat_diff)
  # check for right date-time format und tz
  tz(df.rsf$Dat_Zeit_utc);  class(df.rsf$Dat_Zeit_utc); range(df.rsf$Dat_Zeit_utc)
  df.rsf %>%  filter( Dat_Zeit_utc == "2001-10-22 00:00:00 UTC")
  sum(duplicated(df.rsf$Dat_Zeit))
  # # check data consistense
  table(df.rsf$Kan?le)



## Data Manipulation
  # 1. check for double entries and remove them
  if (dup_check != 0){
    dup_rsf <- df.rsf[duplicated(df.rsf$Dat_Zeit),]
  }
  df.rsf2 <- df.rsf %>%  filter(!duplicated(df.rsf$Dat_Zeit_utc))
  
  # 2. drop all coloums containing only NA values and non relevant information
  df.rsf3 <- df.rsf2[,which(unlist(lapply(df.rsf2, function(x) !all(is.na(x)))))] 
    # %>%
    # select(-c(Proto_Dat, Dat_Zeit_utc))
  
  # 3. write new time coloum
  df.rsf4 <- df.rsf3 %>%  
    mutate("Dat_Zeit" = strftime(df.rsf3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC")) 
  
## Dataexport as .dat (txt) in Delta T Loggerformat
# write.fwf2(df.rsf4, file="W:/Datenbank Level2/Import DB/Rosenfeld_altendatencombi.dat", year= year(range(df.rsf$Dat_Zeit_utc)[1]))
  
###################################### ROTENFELS ########################################

## set odbc connection to acces db
path.db <- "ROTENFELS"
# open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
## Get data from db
SQLQuery_rot <- paste0(
  "SELECT * FROM LoggerTab_Rotenfels")
df.rot <- sqlQuery(rodbc.connect, SQLQuery_rot, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
  arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()
## Check Data
  # 1. check duplicated entries
  dup_check <- sum(duplicated(df.rot$Dat_Zeit_utc))
  # 2. check for missing data
  gaps_rot <- check_for_ts_gaps(ts= df.rot$Dat_Zeit, max_diff= 24*60 )
  class(gaps_rot$Dat_diff)
  as.difftime(gaps_rot$Dat_diff, format= "%H:%M")
  as.numeric(gaps_rot$Dat_diff, units = "days")
  format(gaps_rot$Dat_diff)
  # check for right date-time format und tz
  tz(df.rot$Dat_Zeit_utc);  class(df.rot$Dat_Zeit_utc); range(df.rot$Dat_Zeit_utc)
  df.rot %>%  filter( Dat_Zeit_utc == "2001-10-22 00:00:00 UTC")
  sum(duplicated(df.rot$Dat_Zeit))
  # # check data consistense
  table(df.rot$RO_Kan?le)


## Data Manipulation
  # 1. check for double entries and remove them
  if (dup_check != 0){
    dup_rot <- df.rot[duplicated(df.rot$Dat_Zeit),]
  }
  df.rot2 <- df.rot %>%  filter(!duplicated(df.rot$Dat_Zeit_utc))
  
  # 2. drop all coloums containing only NA values and non relevant information
  df.rot3 <- df.rot2[,which(unlist(lapply(df.rot2, function(x) !all(is.na(x)))))] 
    # %>%
    # select(-c(Proto_Dat, Dat_Zeit_utc))
  
  # 3. write new time coloum
  df.rot4 <- df.rot3 %>%  
    mutate("Dat_Zeit" = strftime(df.rot3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC")) 
  
## Dataexport as .dat (txt) in Delta T Loggerformat
# write.fwf2(df.rot4, file="W:/Datenbank Level2/Import DB/Rotenfels_altendatencombi.dat", year= year(range(df.rot$Dat_Zeit_utc)[1]))

###################################### Tuttlingen #######################################################

## set odbc connection to acces db
path.db <- "Tuttlingen"
# open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
## Get data from db
SQLQuery_tut <- paste0(
  "SELECT * FROM Starlogger")
df.tut <- sqlQuery(rodbc.connect, SQLQuery_tut, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
  arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()
## Check Data
  # 1. check duplicated entries
  dup_check <- sum(duplicated(df.tut$Dat_Zeit_utc))
  # 2. check for missing data
  gaps_tut <- check_for_ts_gaps(ts= df.tut$Dat_Zeit, max_diff= 24*60 )
  class(gaps_tut$Dat_diff)
  as.difftime(gaps_tut$Dat_diff, format= "%H:%M")
  as.numeric(gaps_tut$Dat_diff, units = "days")
  format(gaps_tut$Dat_diff)
  # check for right date-time format und tz
  tz(df.tut$Dat_Zeit_utc);  class(df.tut$Dat_Zeit_utc); range(df.tut$Dat_Zeit_utc)
  df.tut %>%  filter( Dat_Zeit_utc == "2001-10-22 00:00:00 UTC")
  sum(duplicated(df.tut$Dat_Zeit))
  # # check data consistense
  table(df.tut$Kan?le)


## Data Manipulation
  # 1. check for double entries and remove them
  if (dup_check != 0){
    dup_tut <- df.tut[duplicated(df.tut$Dat_Zeit),]
  }
  df.tut2 <- df.tut %>%  filter(!duplicated(df.tut$Dat_Zeit_utc))
  
  # 2. drop all coloums containing only NA values and non relevant information
  df.tut3 <- df.tut2[,which(unlist(lapply(df.tut2, function(x) !all(is.na(x)))))] 
    # %>%
    # select(-c(Proto_Dat, Dat_Zeit_utc))
  
  # 3. write new time coloum
  df.tut4 <- df.tut3 %>%  
    mutate("Dat_Zeit" = strftime(df.tut3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC")) 

## Dataexport as .dat (txt) in Delta T Loggerformat
# write.fwf2(df.tut4, file="W:/Datenbank Level2/Import DB/Tuttlingen_altendatencombi.dat", year= year(range(df.tut$Dat_Zeit_utc)[1]))

######################################### WELZHEIM ###############################################

## set odbc connection to acces db
path.db <- "Welzheim"
# open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
## Get data from db
SQLQuery_wlz <- paste0(
  "SELECT * FROM Starlogger")
df.wlz <- sqlQuery(rodbc.connect, SQLQuery_wlz, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit_utc = ymd_hms(Dat_Zeit, tz="UTC")) %>% # get column with new time format
  arrange(Dat_Zeit_utc) %>%   collect()
odbcCloseAll()
## Check Data
# 1. check duplicated entries
dup_check <- sum(duplicated(df.wlz$Dat_Zeit_utc))
# 2. check for missing data
gaps_wlz <- check_for_ts_gaps(ts= df.wlz$Dat_Zeit, max_diff= 24*60 )
class(gaps_wlz$Dat_diff)
as.difftime(gaps_wlz$Dat_diff, format= "%H:%M")
as.numeric(gaps_wlz$Dat_diff, units = "days")
format(gaps_wlz$Dat_diff)
# check for right date-time format und tz
tz(df.wlz$Dat_Zeit_utc);  class(df.wlz$Dat_Zeit_utc); range(df.wlz$Dat_Zeit_utc)
df.wlz %>%  filter( Dat_Zeit_utc == "2001-10-22 00:00:00 UTC")
sum(duplicated(df.wlz$Dat_Zeit))
# # check data consistense
table(df.wlz$Kan?le)


## Data Manipulation
# 1. check for double entries and remove them
if (dup_check != 0){
  dup_wlz <- df.wlz[duplicated(df.wlz$Dat_Zeit),]
}
df.wlz2 <- df.wlz %>%  filter(!duplicated(df.wlz$Dat_Zeit_utc))

# 2. drop all coloums containing only NA values and non relevant information
df.wlz3 <- df.wlz2[,which(unlist(lapply(df.wlz2, function(x) !all(is.na(x)))))] 
  # %>%
  # select(-c(Proto_Dat, Dat_Zeit_utc))

# 3. write new time coloum
df.wlz4 <- df.wlz3 %>%  
  mutate("Dat_Zeit" = strftime(df.wlz3$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC")) 

## Dataexport as .dat (txt) in Delta T Loggerformat
# write.fwf2(df.wlz4, file="W:/Datenbank Level2/Import DB/wlztlingen_altendatencombi.dat", year= year(range(df.wlz$Dat_Zeit_utc)[1]))



##################################################################################################################################
## cLOSE ODBC and export data  


write.csv2( gaps_alt, file= "W:/Datenbank Level2/Fehler/L?cken_Altensteig_Altdaten_acess.csv")

write.csv2(gaps_blau, file= "W:/Datenbank Level2/Fehler/L?cken_Blauen_Altdaten_acess.csv")

write.csv2( gaps_mud, file= "W:/Datenbank Level2/Fehler/L?cken_Mudau_Altdaten_acess.csv")

write.csv2( gaps_och, file= "W:/Datenbank Level2/Fehler/L?cken_Ochsenhausen_Altdaten_acess.csv")

write.csv2( gaps_rot, file= "W:/Datenbank Level2/Fehler/L?cken_Rotenfels_Altdaten_acess.csv")

write.csv2( gaps_rsf, file= "W:/Datenbank Level2/Fehler/L?cken_Rosenfeld_Altdaten_acess.csv")

write.csv2( gaps_tut, file= "W:/Datenbank Level2/Fehler/L?cken_Tuttlingen_Altdaten_acess.csv")

write.csv2( gaps_wlz, file= "W:/Datenbank Level2/Fehler/L?cken_Welzheim_Altdaten_acess.csv")
#---------------------------##
  


export_each_year(df = df.alt4, name = "Alt_Bruhberg_combi")

export_each_year(df = df.blau4, name = "Blauen_combi")

export_each_year(df = df.mud4, name = "Mudau_combi")

export_each_year(df = df.ess4, name = "Esslingen_combi")


export_each_year(df = df.och4, name = "Ochsenhausen_combi")

export_each_year(df = df.rsf4, name = "Rosenfeld_combi")

export_each_year(df = df.rot4, name = "Rotenfels_combi")

export_each_year(df = df.tut4, name = "Tuttlingen_combi")

export_each_year(df = df.wlz4, name = "Welzheim_combi")



tock <- Sys.time()



  
#  system('shutdown -s')
range(df.alt4$Dat_Zeit_utc)
range(df.och4$Dat_Zeit_utc)
range(df.rot4$Dat_Zeit_utc)
