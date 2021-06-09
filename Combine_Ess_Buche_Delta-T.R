library(tidyverse)
library(lubridate)
library(data.table)
library(gdata)

setwd("O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Buche_DeltaT/backup.dat/2020")

#load all dat-file from directory
tbl <- list.files(pattern = "*.dat")

tbl1 <- lapply(tbl, function(x){
  #x <- tbl[[1]]
  df <- read.csv(x, skip = 10, strip.white = T)
  #df <- read_csv(x, skip = 7) %>% slice( -c(1,2,3))
})


#headers <- read.csv(tbl[[1]], skip = 7, nrows =1, strip.white = T, stringsAsFactors = FALSE, header= F)
h <- c("Label            ",26," ","  RegenY"," ","  RegenZ"," ","  FDR60z"," ","  FDR60y"," ","  FDR60x"," ","   PF60z"," ","   PF60y"," ","   PF60x"," ","  FDR30z"," ","  FDR30y"," ","  FDR30x"," ","   PF30z"," ","   PF30y"," ","   PF30x"," ","   SE889"," ","   SE907"," ","   SE897"," ","   SE915"," ","   SE901"," ","   SE908"," ","Temp_889"," ","Temp_915"," ","Temp_897"," ","Temp_904"," ","Temp_908"," ","Temp_901")
hn <- nchar(h)
n <- trimws(h)


tbl2 <- lapply(tbl1, function(x){
  setNames(x, n) 
})

lapply(tbl2, function(x){
  ncol(x) 
})



tbl3 <- rbindlist(tbl2)


write.fwf2 <- function(d, file, units) {
  opts <- options(useFancyQuotes = FALSE)
  on.exit(options(opts))
  h1 <- dQuote("DELTA-T LOGGER")
  h2 <- dQuote("Esslingen Fichte Delta-T 2020")
  h3 <- range(d$Label)[1]
  h4 <- range(d$Label)[2]
  h5 <- dQuote("TIMED")
  h6 <- paste(dQuote(c("Channel number", rep("", ncol(d)))), collapse = ",")
  h7 <- paste(dQuote(c("Sensor code",    rep("", ncol(d)))), collapse = ",")
  h8 <- paste(dQuote(h), collapse= ",")
  h9 <- paste(dQuote(c("Units",          rep("", ncol(d)))), collapse = ",")
  h10 <- paste(dQuote(c("Minimum value", rep("", ncol(d)))), collapse = ",")
  h11 <- paste(dQuote(c("Maximum value", rep("", ncol(d)))), collapse = ",")
  writeLines(paste(h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11, sep = "\n"), file,)
  write.fwf(d, file, sep = ",", append = TRUE, quote = T,colnames =F, width = hn)
}


write.fwf2(tbl3, file = "Ess_Level2BU_DeltaT_combi.dat")
