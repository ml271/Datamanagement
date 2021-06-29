# df <- dat
# summary(df)
# df[,5]
# i = 5
delt_col_only_na <- function(df){
  
  nrow(df[1]) -> dl.row
  ncol(df) -> dl.col
  #rm(m.selc)
  m.selc <- character()
  
  for(i in 1:dl.col){
    
    if(
      sum(is.na(df[i])) != dl.row
      )
      
      {
      m.selc[i] <- names(df[i])
    }
  }
  
  df <- df %>% 
    select(m.selc[is.na(m.selc)== F]) %>%
    mutate("Datum" = strftime(df$Datum, format = '%d/%m %H:%M:%S', tz="UTC"))
  
return(df)
}