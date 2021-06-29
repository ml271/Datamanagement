library(lubridate)
library(dplyr)
library(RODBC)
library(xlsx)
library(gdata)
library(RODBC)
library(LoggerImports)
# int.Year <- 2020 # specify year of interesst

#try odbc connection to acces db

path.db <- "O:/PROJEKT/CONVENT/LOGDATEN/DBDAT/Conventwald"
dl_table <- "DL1_BTA1"
year <- 2021

source("functions/check_for_ts_gaps.R")
source("functions/deleteColumnsNa.R")

######################################################################################

function(path.db, dl_table, year ){
  
  ## open connection
  rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
  
  # get data from db.connection form target year
  SQLQuery_dl1 <- paste0(
    "SELECT * FROM ", dl_table, " WHERE Dat_Zeit BETWEEN #01/01/",year,"# AND #31/12/",year," 23:59:59#")
  dat <- sqlQuery(rodbc.connect, SQLQuery_dl1, stringsAsFactors = FALSE ,as.is=T) %>%
    mutate(Datum = ymd_hms(Dat_Zeit)) %>% 
    arrange(Datum) %>% collect() %>% select(-Dat_Zeit) %>% select(Datum, everything())
  
  #checking for duplicated entries
  dup_check <- sum(duplicated(dat$Datum))
  if (dup_check != 0){
    dup_dat <- dat[duplicated(dat$Datum),]
  }
  #checking for data gaps
  check_for_ts_gaps(dat$Datum, max_diff = 24*60)
  summary(dat)
  
  #delete columns iwth only Na values
  dat <- delt_col_only_na(dat) %>% select(-Proto_Dat)
  
  
}
  

#----------------------------------------------------------------------
#######################################################################

all.dl5 <- all.dl4
all.dl5[[1]] <- all.dl4[[1]] %>% select(-c(cobunord,cobusost,cobuswes, cofisued, cofinord, Proto_Dat)) %>%  select(Datum, CO1_Kan?le, x_BREGENXX, BREGENXX, x_LogTempB, LogTempB, everything())
all.dl5[[2]] <- all.dl4[[2]] %>% select(-c(Proto_Dat)) %>% select(Datum, CO2_Kan?le, x_LogTempA, LogTempA, everything())
all.dl5[[3]] <- all.dl4[[3]] %>% select(-c(Proto_Dat))
all.dl5[[4]] <- all.dl4[[4]] %>% select(-c(Proto_Dat, Temp_898, Temp_881, Temp_899))

all.dl6 <- all.dl5

names(all.dl6[[1]])[seq(from=3, to=length(names(all.dl6[[1]])), by =2)] <- " "
names(all.dl6[[2]])[seq(from=3, to=length(names(all.dl6[[2]])), by =2)] <- " "
names(all.dl6[[3]])[seq(from=3, to=length(names(all.dl6[[3]])), by =2)] <- " "
names(all.dl6[[4]])[3] <- " "


write.fwf2 <- function(d, file, units) {
  opts <- options(useFancyQuotes = FALSE)
  on.exit(options(opts))
  h1 <- "DELTA-T LOGGER"
  h2 <- "names(d)"
  h3 <- range(d$Datum)[1]
  h4 <- range(d$Datum)[2]
  h5 <- "TIMED"
  h6 <- paste(c("Channel number", rep("", ncol(d))), collapse = ",")
  h7 <- paste(c("Sensor code",    rep("", ncol(d))), collapse = ",")
  h8 <- paste(names(d), collapse= ",")
  h9 <- paste(c("Units",          rep("", ncol(d))), collapse = ",")
  h10 <- paste(c("Minimum value", rep("", ncol(d))), collapse = ",")
  h11 <- paste(c("Maximum value", rep("", ncol(d))), collapse = ",")
  writeLines(paste(h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11, sep = "\n"), file,)
  write.fwf(d, file, sep = ",", append = TRUE, quote = F,colnames =F, width = c )
}



#--------------------------------------------------------------------------------------
b <- c("Label            ",12," ","BREGENXX"," ","LogTempB"," ","BTA1XT01"," ","BTA1XT03"," ","BTA1XT06"," ","BTA1XT12"," ","BTA1XT18"," ","BTA1ZT01"," ","BTA1ZT03"," ","BTA1ZT06"," ","BTA1ZT12"," ","BTA1ZT18")
c <- nchar(b)
a <- names(all.dl6[[1]])

for( i in 1: length(a)){
  a[i] <- sprintf(paste0("%-", c[i], "s"), a[i])
}
names(all.dl6[[1]]) <- a

write.fwf2(all.dl6[[1]], file=paste0("export_form_conventwald.mdb/",names(all.dl6[1]), "combi2-2020.dat"))
#--------------------------------------------------------------------------------------
b <- c("Label            ", 7," ","LogTempA"," ","BLI1ZT01"," ","BLI1ZT03"," ","BRELUB1X"," ","BTEMPB1X"," ","BRELUB2X"," ","BTEMPB2X")
c <- nchar(b)
a <- names(all.dl6[[2]])

for( i in 1: length(a)){
  a[i] <- sprintf(paste0("%-", c[i], "s"), a[i])
}
names(all.dl6[[2]]) <- a

write.fwf2(all.dl6[[2]], file=paste0("export_form_conventwald.mdb/",names(all.dl6[2]), "combi2-2020.dat"))
#--------------------------------------------------------------------------------------
b <- c("Label            ", 7," ","FWINDXXX"," ","FREGENXX"," ","FReLuxxx"," ","FTempxxx"," ","FGlobalX")
c <- nchar(b)
a <- names(all.dl6[[3]])

for( i in 1: length(a)){
  #i =5
  a[i] <- sprintf(paste0("%-", c[i], "s"), a[i])
}
names(all.dl6[[3]]) <- a
write.fwf2(all.dl6[[3]], file=paste0("export_form_conventwald.mdb/",names(all.dl6[3]), "combi2-2020.dat"))
#-------------------------------------------------------------------------------------
b <- c("Label            ", 1," ","bbu5stam")
c <- nchar(b)
a <- names(all.dl6[[4]])

for( i in 1: length(a)){
  a[i] <- sprintf(paste0("%-", c[i], "s"), a[i])
}
names(all.dl6[[4]]) <- a
write.fwf2(all.dl6[[4]], file=paste0("export_form_conventwald.mdb/",names(all.dl6[4]), "combi2-2020.dat"))
#-------------------------------------------------------------------------------------
# oberer teil wird f?r jedes listen element ncoh einzeln ausgef?hrt da immer andere b (headers) n?tig sind
# all.dl7 <- lapply(all.dl6, function(x){
#   x <- all.dl6[[1]]
#   a <- names(x)
#   x[1]
# }



tail(all.dl6[[2]])
tail(df.dl2)

#write.fwf2(all.dl6[[1]], "W:/Datenbank Level2/Import DB/DELTA T/test3.dat")

sapply(names(all.dl6), function(x){
  
})



plot(all.dl6[[1]]$BREGENXX)
plot(all.dl6[[1]]$LogTempB)
plot(all.dl6[[1]]$BTA1XT01, type="l")
plot(all.dl6[[1]]$BTA1ZT18, type="l")
