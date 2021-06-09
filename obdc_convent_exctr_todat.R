library(lubridate)
library(dplyr)
library(RODBC)
library(xlsx)
# 
# int.Year <- 2020 # specify year of interesst

#try odbc connection to acces db
setwd("O:/PROJEKT/CONVENT/LOGDATEN/DBDAT/")
path.db <- "Conventwald"
## open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")


SQLQuery_dl1 <- paste0(
  "SELECT * FROM DL1_BTA1 WHERE Dat_Zeit BETWEEN #01/01/2020# AND #31/12/2020#")
df.dl1 <- sqlQuery(rodbc.connect, SQLQuery_dl1, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit = ymd_hms(Dat_Zeit)) %>%  collect()
df.dl1$Dat_Zeit

SQLQuery_dl2 <- paste0(
  "SELECT * FROM DL2_BFI2 WHERE Dat_Zeit BETWEEN #01/01/2020# AND #31/12/2020#")
df.dl2 <- sqlQuery(rodbc.connect, SQLQuery_dl2, stringsAsFactors = FALSE, as.is=T) %>%
  mutate(Dat_Zeit = ymd_hms(Dat_Zeit)) %>% collect()
df.dl2$Dat_Zeit

SQLQuery_dl3 <- paste0(
  "SELECT * FROM DL3_WFI2 WHERE Dat_Zeit BETWEEN #01/01/2020# AND #31/12/2020#")
df.dl3 <- sqlQuery(rodbc.connect, SQLQuery_dl3, stringsAsFactors = FALSE, as.is = T) %>%
  mutate(Dat_Zeit = ymd_hms(Dat_Zeit)) %>%  collect()
df.dl3$Dat_Zeit


###### not relevant for 2020!!!
# 
# SQLQuery_dl4 <- paste0(
#   "SELECT * FROM DL4_WFI4 WHERE Dat_Zeit BETWEEN ", paste0("#01/01/", int.Year, "#")," AND", paste0("#31/12/", int.Year, "#"))
# df.dl4 <- sqlQuery(rodbc.connect, SQLQuery_dl4, stringsAsFactors = FALSE) %>% collect()
# 
# 
# SQLQuery_dl5 <- paste0(
#   "SELECT * FROM DL5_WFEN WHERE Dat_Zeit BETWEEN ", paste0("#01/01/", int.Year, "#")," AND", paste0("#31/12/", int.Year, "#"))
# df.dl5 <- sqlQuery(rodbc.connect, SQLQuery_dl5, stringsAsFactors = FALSE) %>% collect()
# 


SQLQuery_dl6 <- paste0(
  "SELECT * FROM DL6_BBU5 WHERE Dat_Zeit BETWEEN #01/01/2020# AND #31/12/2020#")
df.dl6 <- sqlQuery(rodbc.connect, SQLQuery_dl6, stringsAsFactors = FALSE, as.is = T) %>%
  mutate(Dat_Zeit = ymd_hms(Dat_Zeit)) %>%
  collect()
df.dl6$Dat_Zeit


#####################################
odbcCloseAll()

# combine alll dfs into one list
all.dl <- list(df.dl1, df.dl2, df.dl3, df.dl6)
names(all.dl) <- c("DL1_BTA1", "DL2_BFI2", "DL3_WFI2", "DL6_BBU5")
rm(list=setdiff(ls(), c("all.dl","int.Year")))

all.dl[[1]]
#------------------------------------------------------------------------------------------------

# select only coulumns with measurement data, exclude flag coulmns with "x_..."
all.dl3 <- lapply(all.dl, function(x){
  c.names <- data.frame(x = colnames(x))
  n.selc <- c.names %>% filter(substring(x,1,2) != "x_") %>% pull()
  x %>%  select(n.selc)
  
})



# select only coulmns that store data, deleting coulmns with only "NA"
all.dl4 <- lapply(all.dl, function(x){
  nrow(x[1]) -> dl.row
  ncol(x) -> dl.col
  #rm(m.selc)
  m.selc <- character()
  
  for(i in 1:dl.col){
    
    if(
      sum(is.na(x[i])) != dl.row
    ){
      m.selc[i] <- names(x[i])
    }
  }
  x %>% 
    select(m.selc[is.na(m.selc)== F]) %>%
    mutate("Label" = strftime(x$Dat_Zeit, format = '%d/%m %H:%M:%S')) %>% 
    select(Label, everything()) 
  #select(-c(CO1_Kan?le, cobunord, cobusost, cobuswes, cofinord,cofisued, Proto_Dat,  LogTempB       ))
  
})
all.dl4[[1]]

# # delete coulmns with other nonsense for each df in list individualy
# all.dl4[[1]] <- all.dl4[[1]] %>% select(-c(CO1_Kanäle, cobunord, cobusost, cobuswes, cofinord,cofisued, Proto_Dat,  ))
# all.dl4[[2]] <- all.dl4[[2]] %>% select(-c(CO2_Kanäle,Proto_Dat,))
# all.dl4[[3]] <- all.dl4[[3]] %>% select(-c(CO3_Kanäle,Proto_Dat))
# # all.dl4[[4]] 
# # all.dl4[[5]]
# all.dl4[[4]] <- all.dl4[[4]] %>%  select(-c(Proto_Dat))



write.csv3 <- function(d, file, units) {
  opts <- options(useFancyQuotes = FALSE)
  on.exit(options(opts))
  h1 <- dQuote("DELTA-T LOGGER")
  h2 <- dQuote("names(d)")
  h3 <- dQuote(range(d$Dat_Zeit)[1])
  h4 <- dQuote(range(d$Dat_Zeit)[2])
  h5 <- dQuote("TIMED")
  h6 <- paste(dQuote(c("Channel number","12", rep("", ncol(d)-1))), collapse = ",")
  h7 <- paste(dQuote(c("Sensor code", "12", rep("", ncol(d)-1))), collapse = ",")
  d <- d %>% select(-Dat_Zeit)
  h8 <- paste(names(d), collapse = ",")
  h9 <- paste(dQuote(c("Units", "12", rep("", ncol(d)))), collapse = ",")
  h10 <- paste(dQuote(c("Minimum value","12", rep("", ncol(d)))), collapse = ",")
  h11 <- paste(dQuote(c("Maximum value","12", rep("", ncol(d)))), collapse = ",")
  writeLines(paste(h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11, sep = "\n"), file,)
  write.table(d, file, sep = ",", dec=".", append = TRUE, col.names = F, row.names = F, quote = F)
}



write.csv4 <- function(d, file, units) {
  opts <- options(useFancyQuotes = FALSE)
  on.exit(options(opts))
  h1 <- dQuote("DELTA-T LOGGER")
  h2 <- dQuote("names(d)")
  h3 <- dQuote(range(d$Dat_Zeit)[1])
  h4 <- dQuote(range(d$Dat_Zeit)[2])
  h5 <- dQuote("TIMED")
  h6 <- paste(dQuote(c("Channel number   ",12," ",        62," ",         1," ",         2," ",         3," ",         4," ",         5," ",         6," ",         7," ",         8," ",         9," ",        10," ",        11)), collapse = ",")
  h7 <- paste(dQuote(c("Sensor code      ",12," ","     CNT"," ","     TM1"," ","     VLT"," ","     VLT"," ","     VLT"," ","     VLT"," ","     VLT"," ","     TEN"," ","     VLT"," ","     VLT"," ","     VLT"," ","     VLT")), collapse = ",")
  d <- d %>% select(-Dat_Zeit)
  h8 <- paste(dQuote(c("Label            ",12," ","BREGENXX"," ","LogTempB"," ","BTA1XT01"," ","BTA1XT03"," ","BTA1XT06"," ","BTA1XT12"," ","BTA1XT18"," ","BTA1ZT01"," ","BTA1ZT03"," ","BTA1ZT06"," ","BTA1ZT12"," ","BTA1ZT18")), collapse = ",")
  h9 <- paste(dQuote(c("Unit             ",12," ","  xxxxmm"," ","   deg C"," ","     hPa"," ","     hPa"," ","     hPa"," ","     hPa"," ","     hPa"," ","     hPa"," ","     hPa"," ","     hPa"," ","     hPa"," ","     hPa")), collapse = ",")
  h10 <- paste(dQuote(c("Minimum value    ",12," ",     0.000," ",      0.71," ",    -149.3," ",   -172.98," ",   -92.932," ",  -145.294," ",     -99.5," ",    -70.77," ",      7.21," ",    -26.40," ",    -77.06," ",   -209.57)), collapse = ",")
  h11 <- paste(dQuote(c("Maximum value    ",12," ",     8.690," ",     29.04," ",     703.5," ",     811.2," ",     807.7," ",     747.8," ",     138.4," ",     625.8," ",     802.0," ",     770.4," ",     350.0," ",     926.9)), collapse = ",")
  writeLines(paste(h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11, sep = "\n"), file,)
  write.table(d, file, sep = ",", dec=".", append = TRUE, col.names = F, row.names = F, quote = F)
}
write.csv4(all.dl4[[1]], file=paste0("export_form_conventwald.mdb/", names(all.dl4[1]), ".dat"))
#write all df in list to csv data in wd/export....
sapply(names(all.dl4), function(x){
  write.csv3(all.dl4[[x]], file=paste0("export_form_conventwald.mdb/", names(all.dl4[x]), ".dat"))
})

tail(all.dl4[[4]])
######### Eigentlich sollte gleich XLSX Output ############

d <- all.dl4[[4]]
units <- rep("UNIT", 1, length(names(d)))


# funktioniert noch nicht
write.xlsx3 <- function(d, file, units) {
  opts <- options(useFancyQuotes = FALSE)
  on.exit(options(opts))
  d2 <- rbind(names(d), units)
  write.xlsx2(d2, file =file, col.names = F, row.names = F)
  write.xlsx2(d, file =file, append = T,  col.names = F, row.names = F )
}


write.xlsx3(all.dl4[[4]], file=paste0("export_form_conventwald.mdb/", names(all.dl4[4]), ".xlsx"), units = rep("UNIT", 1, length(names(all.dl4[[4]]))) )

#write all df in list to csv data in wd/export....
sapply(names(all.dl4), function(x){
  write.xlsx3(all.dl4[[x]], file=paste0(names(all.dl4[x]), ".xlsx"), units = rep("UNIT", 1, length(names(all.dl4[[x]]))))
})


