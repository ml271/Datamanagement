########################################################################
######################   Combine ESSLIINGEN FICHTE ENVILOG #################################
########################################################################
# autor: Marvin Lorff
# date: 18.01.2021
# version: 02.01



# TODO: convert data into long format
# TODO: load all plots/Subplots at once
# TODO: convert skript to function


# load functions and libraries
setwd("W:/R/Datamanagement")
library(tidyverse)
library(lubridate)


source("functions/readEnvilog.R")
source("functions/check_for_ts_gaps.R")
source("functions/comtodot.R")
source("functions/countna.R")

#set directory to the Esslingen (ENVILOG) and the choosen Year
plot_name<- "Esslingen"
subplot_name <- "Fichte"
path <- paste0("O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Fichte_envilog/", vch.Year)
LoggerExport = T # erzeugt eine Datei im path_out,
# welche einer Loggerdatei des jeweilige Formats entspricht,
# und so über die Web oberfläche der Datenbank hochgeladen werden kann
path_out <- "W:/R/Datamanagement/data" # defriniert den outpath für die Loggerdatei
long_data <- T #  speichert die daten in R im "long-format"
# w?hle das jahr das zusammengefasst werden soll
Year <- 2021
#-----------------------------------------------------------------------------------
### Initialies funktion to run

combine_Envilog_files <- function(path, plot_name, subplot_name, Year, LoggerExport, path_out, long_data =T){
  #get all csv-file paths from directory
  l.paths <- list.files(path = path, pattern = "*.csv", full.names = T)
  #gather data form files, use readEnvilog from LoggerImports
  #to make sure Sensors have identical and consistent colomns names
  dat <- l.paths %>% map_df( ~ readEnvilog(.))
  #-----------------------------------------------------------------------------------
  
  ### Check Data
  # check time and other column classes
  str(dat)
  # check data ranges and nas
  summary(dat)
  # 1. check duplicated entries
  dup_check <- sum(duplicated(dat$Datum))
  if (dup_check != 0){
    dup_dat <- dat[duplicated(dat$Datum),]
    #dat <- dat %>%  filter(!duplicated(dat$Datum))
  }
  
  
  # 2. check for missing data
  gaps <- check_for_ts_gaps(ts= dat1$Datum, max_diff= 24*60 )
  # class(gaps_alt$Dat_diff)
  # as.difftime(gaps_alt$Dat_diff, format= "%H:%M")
  # as.numeric(gaps_alt$Dat_diff, units = "days")
  # format(gaps_alt$Dat_diff)
  
  # 3. check for right date-time format und tz
  tz(dat$Datum);  class(dat$Datum); range(dat$Datum)
  dat %>%  filter( Datum >= "2021-01-01 00:00:00 UTC")
  sum(duplicated(dat$Datum))
  # 4. check data consistency
  #table(dat$Kanäle)
  
  #--------------------------------------------------------------------------------------
  
  
  # edit data, date time format, kick duplicated entries and filter for the choosen Year
  dat1 <- 
    dat %>% arrange(Datum) %>%
    distinct(Datum, .keep_all= T) %>%
    #filter(!duplicated(dat$Datum)) %>%
    filter(year(Datum) >= vch.Year) %>% 
    filter(year(Datum) < vch.Year+1)
  
  #test
  countna(dat1$Datum)
  print(paste("Es wurden Daten vom" , range(dat1$Datum, na.rm=T)[1], "bis zum", range(dat1$Datum, na.rm=T)[2], "gefunden und zusammengefasst"))
  head(dat1)
  str(dat1)
  
  l <- unique(date(dat1$Datum))
  t <- seq.Date(from = ymd(paste0(vch.Year, "-01-01")), to = ymd(paste0(vch.Year, "-12-31")), by = 1)
  print(paste(" Von", length(t), "Tagen im Jahr", vch.Year," wurden", sum(l == t), "aufeinanderfolgenden Tage zusammengefasst." ))# alle Tage vorhanden!
  
  if (long_data == T){
    # prepare data in R-Format
    dat_r <- dat1 %>% pivot_longer(. , cols= -Datum,  names_to = "value", values_to = "messwert") %>% 
      mutate(plot = plot_name, subplot = subplot_name) %>% 
      select(Datum, plot, subplot, messwert, value)
    return(dat_r)
  }
  else(
    return(dat1)
  )
  
  if (LoggerExport == T){
    # prepare data for export in Logger-format
    dat_exp <- dat1 %>% 
      mutate(Datum = format(dat1$Datum, format = "%d.%m.%Y %H:%M")) %>% 
      distinct() %>% 
      mutate(No = seq(1:nrow(.))) %>% select(No, everything())
    # Export data
    # erstelle ein zusammengefasste tabelle f?r das jeweilige jahr
    writeLines("Logger: #D3000C 'Esslingen_Fi_FVA_1' - USP_EXP2 - (CGI) Expander for GP5W - (V2.60, Mai 12 2013)", paste0("data/ES_Level2FI_Envilog__", vch.Year,"_combine.csv"))
    write.table(dat_exp, file=paste0("data/ES_Level2FI_Envilog__", vch.Year,"_combine.csv"), sep = ";", dec=",", col.names = TRUE, append= TRUE, quote = F, row.names = F, na = "")
    print(paste( "Es wurde eine Logger-Combi-Datei datei für das Jahr", vch.Year, "erstellt und unter", path_out, "gespeichert."))
  }
  
  
}


#testing funktion

combine_Envilog_files(path=path, plot_name = plot_name, subplot_name = subplot_name, Year = Year, LoggerExport = LoggerExport, long_data = long_data, path_out = path_out)
