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
#------------------------------------------------------------------------------------------------

# select only coulumns with measurement data, exclude flag coulmns with "x_..."
all.dl3 <- lapply(all.dl, function(x){
  c.names <- data.frame(x = colnames(x))
  n.selc <- c.names %>% filter(substring(x,1,2) != "x_") %>% pull()
  x %>%  select(n.selc)
  
})

# select only coulmns that store data, deleting coulmns with only "NA"
all.dl4 <- lapply(all.dl3, function(x){
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
    mutate("Time" = strftime(x$Dat_Zeit, format = '%d.%m.%Y %H:%M:%S')) %>% 
    mutate("No" = seq(1:nrow(x))) %>% select(No,Time, everything()) %>% select(-Dat_Zeit)
  #select(-c(CO1_Kan?le, cobunord, cobusost, cobuswes, cofinord,cofisued, Proto_Dat,  LogTempB       ))
  
})


# delete coulmns with other nonsense for each df in list individualy
all.dl4[[1]] <- all.dl4[[1]] %>% select(-c(CO1_Kanäle, cobunord, cobusost, cobuswes, cofinord,cofisued, Proto_Dat,  ))
all.dl4[[2]] <- all.dl4[[2]] %>% select(-c(CO2_Kanäle,Proto_Dat,))
all.dl4[[3]] <- all.dl4[[3]] %>% select(-c(CO3_Kanäle,Proto_Dat))
# all.dl4[[4]] 
# all.dl4[[5]]
all.dl4[[4]] <- all.dl4[[4]] %>%  select(-c(Proto_Dat))



write.csv3 <- function(d, file, units) {
  opts <- options(useFancyQuotes = FALSE)
  on.exit(options(opts))
  h1 <- paste(dQuote(c( names(d))), collapse = ";")
  h2 <- paste(dQuote(c( units)), collapse = ";")
  writeLines(paste(h2, sep = "\n"), file,)
  write.table(d, file, sep = ";", dec=",", append = TRUE, col.names = T, row.names = F, quote = F)
}


#write all df in list to csv data in wd/export....
sapply(names(all.dl4), function(x){
  write.csv3(all.dl4[[x]], file=paste0("export_form_conventwald.mdb/", names(all.dl4[x]), "edit.csv"), units= rep("UNIT", 1, length(names(all.dl4[[x]]))))  
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


