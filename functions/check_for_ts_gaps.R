check_for_ts_gaps <- function(ts, max_diff, list=F){
  # 
  # #for testing
  # ts = dat$Datum
  # #
  # max_diff <- 24*60

  #conditions
  if(class(ts)[1] != "POSIXct"){
    
    tryCatch(
      expr = {
        ts <- as.POSIXct(ts)
        message("ts was converted to POSIXct")
      },
      error = function(e){
        message("ts could not be converted to object POSIXct")
        print(e)
      },
      warning = function(w){
        message("Warning:")
        print(w)
      }
    )
    
  }
  # ts <- df.blau2$Dat_Zeit_utc
  # max_diff <- time
  
  df <- data.frame(Dat_Zeit = ts)
  
  #clculate time differences of time step in minutes
  df$Dat_diff <- df$Dat_Zeit -lag(df$Dat_Zeit)
  units(df$Dat_diff) <- "mins"
  # table(df.alt$Dat_diff)
  
  
  # maximale time acceptable time diff
  # i_a = ANFANG DER LÜCKE
  i_a <- df$Dat_diff >= max_diff
  i_a[1] <- FALSE
  
  #i_n = ENDE DER LÜCKE
  i_n <- lead(i_a)
  i_n[length(i_n)] <- FALSE
  
  if( sum(i_n, na.rm=T) >= 1){
    print("Lücke gefunden:")
      df.a <- df[i_a,]
      df.n <- df[i_n,]
      df.gap <- rbind(df.a, df.n) %>%  arrange(Dat_Zeit)
      #df.gap$end <- df.gap$Dat_diff >= max_diff
      df.gap$luecke <- rep(c("BEGIN", "END"), length.out= length(df.gap$Dat_Zeit) )
      #df.gap2 <- df.gap[-length(df.gap$luecke),]
      df.gap$no_gap <- rep(1:(length(df.gap$Dat_Zeit)/2), each=2)
      df.gap <- df.gap %>% select(Dat_Zeit, Dat_diff, luecke, no_gap)
          
      # reture
      
      if(list==T){
        l.gap_alt <- split(df.gap, df.gap$no_gap )
        return(l.gap_alt)
      }
      
      return(df.gap)
  }
  else print("Keine Lücke gefunden!")
}