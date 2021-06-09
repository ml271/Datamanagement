library(lubridate)
library(dplyr)
library(RODBC)
library(xlsx)
library(gdata)
library(RODBC)
# 
# int.Year <- 2020 # specify year of interesst

#try odbc connection to acces db
setwd("O:/PROJEKT/CONVENT/LOGDATEN/DBDAT/")
path.db <- "Conventwald"
## open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")

######################################################################################
SQLQuery_dl1 <- paste0(
  "SELECT * FROM Im_DL1_BTA1 WHERE Dat_Zeit BETWEEN #01/01/2020# AND #31/12/2020 23:59:59#")
df.dl1 <- sqlQuery(rodbc.connect, SQLQuery_dl1, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Dat_Zeit = ymd_hms(Dat_Zeit)) %>% 
  arrange(Dat_Zeit) %>%   collect()

tz(df.dl1$Dat_Zeit)
#check for double entries
sum(duplicated(df.dl1$Dat_Zeit))
#check for missing data
r <- df.dl1$Dat_Zeit -lag(df.dl1$Dat_Zeit) 
df.dl1[c(which(r != 30)-2,which(r != 30)-1,which(r != 30) ,which(r != 30)+1),]
df.dl1 %>%  filter( Dat_Zeit >= "2020-10-22 00:00:00" & Dat_Zeit <= "2020-10-26") %>% print(n=50, na.print = "NA")
str(df.dl1)
######################################################################################
SQLQuery_dl2 <- paste0(
  "SELECT * FROM DL2_BFI2 WHERE Dat_Zeit BETWEEN #01/01/2020# AND #31/12/2020 23:59:59#")
df.dl2 <- sqlQuery(rodbc.connect, SQLQuery_dl2, stringsAsFactors = FALSE, as.is=T) %>%
  mutate(Dat_Zeit = ymd_hms(Dat_Zeit)) %>% collect()

sum(duplicated(df.dl2$Dat_Zeit))
#check for missing data
r <- df.dl2$Dat_Zeit -lag(df.dl2$Dat_Zeit) 
r[which(r != 30)]
df.dl2[which( r != 30),]
df.dl2[c(which(r != 30)-1,which(r != 30)),]
df.dl2 %>%  filter( Dat_Zeit >= "2020-10-25 00:02:00" & Dat_Zeit <= "2020-10-26") %>% print(n=50, na.print = "NA")
str(df.dl2)
#######################################################################################
SQLQuery_dl3 <- paste0(
  "SELECT * FROM DL3_WFI2 WHERE Dat_Zeit BETWEEN #01/01/2021# AND #31/12/2020 23:59:59#")
df.dl3 <- sqlQuery(rodbc.connect, SQLQuery_dl3, stringsAsFactors = FALSE, as.is = T) %>%
  mutate(Dat_Zeit = ymd_hms(Dat_Zeit)) %>%  collect()

sum(duplicated(df.dl3$Dat_Zeit))
#check for missing data
r <- df.dl3$Dat_Zeit -lag(df.dl3$Dat_Zeit) 
r[which(r != 30)]
df.dl3[which( r != 30),]
df.dl3[c(which(r != 30)-1,which(r != 30)),]
df.dl3 %>%  filter( Dat_Zeit >= "2020-10-25 00:02:00" & Dat_Zeit <= "2020-10-26") %>% print(n=50, na.print = "NA")
str(df.dl3)
#######################################################################################
SQLQuery_dl6 <- paste0(
  "SELECT * FROM DL6_BBU5 WHERE Dat_Zeit BETWEEN #01/01/2020# AND #31/12/2020 23:59:59#")
df.dl6 <- sqlQuery(rodbc.connect, SQLQuery_dl6, stringsAsFactors = FALSE, as.is = T) %>%
  mutate(Dat_Zeit = ymd_hms(Dat_Zeit)) %>%
  collect()

sum(duplicated(df.dl3$Dat_Zeit))
#check for missing data
r <- df.dl3$Dat_Zeit -lag(df.dl3$Dat_Zeit) 
r[which(r != 30)]
df.dl3[which( r != 30),]
df.dl3[c(which(r != 30)-1,which(r != 30)),]
df.dl3 %>%  filter( Dat_Zeit >= "2020-10-25 00:02:00" & Dat_Zeit <= "2020-10-26") %>% print(n=50, na.print = "NA")
str(df.dl3)



#####################################
odbcCloseAll()
# combine alll dfs into one list
all.dl <- list(df.dl1, df.dl2, df.dl3, df.dl6)
names(all.dl) <- c("DL1_BTA1", "DL2_BFI2", "DL3_WFI2", "DL6_BBU5")


#all.dl <- all.dl[-1]
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

  x <- x %>% 
    select(m.selc[is.na(m.selc)== F]) %>%
    mutate("Dat_Zeit" = strftime(x$Dat_Zeit, format = '%d/%m %H:%M:%S', tz="UTC"))
  #print(tz(x$Dat_Zeit_X))
  
 
})



all.dl5 <- all.dl4
all.dl5[[1]] <- all.dl4[[1]] %>% select(-c(cobunord,cobusost,cobuswes, cofisued, cofinord, Proto_Dat)) %>%  select(Dat_Zeit, CO1_Kanäle, x_BREGENXX, BREGENXX, x_LogTempB, LogTempB, everything())
all.dl5[[2]] <- all.dl4[[2]] %>% select(-c(Proto_Dat)) %>% select(Dat_Zeit, CO2_Kanäle, x_LogTempA, LogTempA, everything())
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
  h3 <- range(d$Dat_Zeit)[1]
  h4 <- range(d$Dat_Zeit)[2]
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
# oberer teil wird für jedes listen element ncoh einzeln ausgeführt da immer andere b (headers) nötig sind
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
