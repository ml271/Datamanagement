library(tidyverse)
library(lubridate)
library(data.table)
library(gdata)

setwd("O:/PROJEKT/NIEDER/LOGGER/OCHS/Ochsenhausen_Fichte_gedüngt_Delta_T/2020")

#load all dat-file from directory
tbl <- list.files(pattern = "*.dat")

tbl1 <- lapply(tbl, function(x){
 #x <- tbl[[1]]
 df <- read.csv(x, skip = 10, strip.white = T)
 #df <- read_csv(x, skip = 7) %>% slice( -c(1,2,3))
})


#headers <- read.csv(tbl[[1]], skip = 7, nrows =1, strip.white = T, stringsAsFactors = FALSE, header= F)
h <- c("Label            ", 9," ","BoTemp10"," ","BoTemp20"," ","BoTemp30"," ","Theta201"," ","Theta202"," ","w_up_con"," ","Theta801"," ","Theta802"," ","Lys_Akku")
hn <- nchar(h)
n <- trimws(h)


tbl2 <- lapply(tbl1, function(x){
  setNames(x, n) 
})

lapply(tbl2, function(x){
  ncol(x) 
})




tbl3 <- rbindlist(tbl2)

sum(duplicated(tbl3$Label))

tbl3[duplicated(tbl3$Label),]


write.fwf2 <- function(d, file, units) {
  opts <- options(useFancyQuotes = FALSE)
  on.exit(options(opts))
  h1 <- "DELTA-T LOGGER"
  h2 <- "Ochsenhausen Fichte gedüngt Delta-T 2020 combi"
  h3 <- range(d$Label)[1]
  h4 <- range(d$Label)[2]
  h5 <- "TIMED"
  h6 <- paste(c("Channel number", rep("", ncol(d))), collapse = ",")
  h7 <- paste(c("Sensor code",    rep("", ncol(d))), collapse = ",")
  h8 <- paste(h, collapse= ",")
  h9 <- paste(c("Units",          rep("", ncol(d))), collapse = ",")
  h10 <- paste(c("Minimum value", rep("", ncol(d))), collapse = ",")
  h11 <- paste(c("Maximum value", rep("", ncol(d))), collapse = ",")
  writeLines(paste(h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11, sep = "\n"), file,)
  write.fwf(d, file, sep = ",", append = TRUE, quote = T,colnames =F, width = hn)
}


write.fwf2(tbl3, file = "OCH_FiDüeng_DeltaT_combi3.dat")
