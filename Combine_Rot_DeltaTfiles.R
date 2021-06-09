library(tidyverse)
library(lubridate)
library(data.table)
library(gdata)

setwd("O:/PROJEKT/NIEDER/LOGGER/ROTENFEL/Rotenfels_Fichte_DeltaT_neu/backup.dat/2020")

#load all dat-file from directory
tbl <- list.files(pattern = "*.dat")

tbl1 <- lapply(tbl, function(x){
 #x <- tbl[[1]]
 df <- read.csv(x, skip = 10, strip.white = T)
 #df <- read_csv(x, skip = 7) %>% slice( -c(1,2,3))
})


#headers <- read.csv(tbl[[1]], skip = 7, nrows =1, strip.white = T, stringsAsFactors = FALSE, header= F)
h <- c("Label            ",13," ","       L"," ","       K"," ","      KR"," ","     K15"," ","     K30"," ","     K60"," ","    KR15"," ","    KR30"," ","    KR60"," ","     L15"," ","     L30"," ","     L60"," ","    batt")
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
  h2 <- dQuote("names(d)")
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


write.fwf2(tbl3, file = "Rot_Level2Fi_DeltaT_combi.dat")
