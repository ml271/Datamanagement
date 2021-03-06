 # df <- dat_exp
 # year = 2020
 # file = paste0(path_out, abbr.plot, "_Level2",abbr.sub, "_DeltaT__", Year,"_combine.csv")
 # 
#--------------------------------
library(stringr)
library(gdata)
#---------------------------------
write.fwf2 <- function(df, file, year) {
  opts <- options(useFancyQuotes = FALSE)
  on.exit(options(opts))
  
  a <- names(df)
  
  
  b <- unname(sapply(df, function(x) max(nchar(x, type= "width")))) # Breite der Werte
  c <- nchar(a, type = "width") #Breiter der Header
  f <- b > c
  f[is.na(f)] <- FALSE
  e <- numeric()
  
  for( i in 1:length(b)){
    if( f[i] == T){
      e[i] <- b[i]
    }
    else(
      e[i] <- c[i]
    )
  }
  e <- e+1
  aa <- str_pad(a, e, side= "left")
# e[46] <- 8
# e[46]
# a[46]

  h1 <- "DELTA-T LOGGER"
  h2 <- "R-Nachbau der Loggerdateien, um Delta-T Dateien oder anderen Loggerdateiformate mit Flag-Splaten in die Level2 Datenbank hochzuladen. Autor:Marvin Lorff, Ausgabe durch Funktion: write.fwf2()"
  h3 <- range(df$Datum)[1]
  h4 <- range(df$Datum)[2]
  h5 <- paste("Start_Year:", year)
  h6 <- paste(c("Channel number", rep("", ncol(df))), collapse = ",")
  h7 <- paste(c("Sensor code",    rep("", ncol(df))), collapse = ",")
  h8 <- paste(aa, collapse= ",")
  h9 <- paste(c("Units",          rep("", ncol(df))), collapse = ",")
  h10 <- paste(c("Minimum value", rep("", ncol(df))), collapse = ",")
  h11 <- paste(c("Maximum value", rep("", ncol(df))), collapse = ",")
  writeLines(paste(h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11, sep = "\n"), file)
  write.fwf(df, file, sep = ",", append = TRUE, quote = F,colnames =F, width = e )
}

