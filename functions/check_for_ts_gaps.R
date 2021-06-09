check_for_ts_gaps <- function(ts, max_diff, list=F){
  # 
  # #for testing
  # ts = df.wlz$Dat_Zeit
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
  df$Dat_diff <- df$Dat_Zeit -lag(df$Dat_Zeit)
  units(df$Dat_diff) <- "mins"
  # table(df.alt$Dat_diff)
  # maximale time acceptable time diff
  i_l <- df$Dat_diff >= max_diff
  i_l[1] <- FALSE
  #i_l[85] <- TRUE
  i_n <- lead(i_l)
  df.l <- df[i_l,]
  df.n <- df[i_n,]
  df.luecke <- rbind(df.l, df.n) %>%  arrange(Dat_Zeit)
  df.luecke$end <- df.luecke$Dat_diff >= max_diff
  df.luecke$luecke <- rep(c("BEGIN", "END"), length.out= length(df.luecke$Dat_Zeit) )
  df.luecke2 <- df.luecke[-length(df.luecke$luecke),]
  df.luecke2$no_l <- rep(1:(length(df.luecke2$Dat_Zeit)/2), each=2)
  df.luecke3 <- df.luecke2 %>% select(Dat_Zeit, Dat_diff, luecke, no_l)
  # return
  l.lck_alt <- split(df.luecke3,df.luecke3$no_l )
  df.lck_alt <- rbindlist(l.lck_alt)
  df.lck_alt$Dat_diff <- as.difftime(df.lck_alt$Dat_diff,units = "days" )
  
  if(list==T){
    return(l.lck_alt)
  }
  
  return(df.lck_alt)
}