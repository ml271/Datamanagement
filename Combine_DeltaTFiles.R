 ########################################################################
######################   Combine DeltaT #################################
 ########################################################################
# autor: Marvin Lorff
# date: 28.01.2021
# version: 02.02



# TODO: wirte some tests for input parameter and error/warnings
# TODO: load all plots/Subplots at once
# TODO: für den subplot ochsenhausen Fichte ungdüngt werden die Sensor namen nicht umgeschrieben.
#       check LoggerImorts for bug fixing




# load functions and libraries
library(tidyverse)
library(lubridate)


source("functions/readDeltaT.R")
source("functions/check_for_ts_gaps.R")
source("functions/comtodot.R")
source("functions/countna.R")
source("functions/write.fwf2.R")

#set directory to the Esslingen (DeltaT) and the choosen year
# 
# plot_name<- "Esslingen"
# subplot_name <- "Buche"
# # w?hle das jahr das zusammengefasst werden soll
# year <- 2021
# path <- "O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Buche_DeltaT/backup.dat/2021"
# LoggerExport = T # erzeugt eine Datei im path_out,
# # welche einer Loggerdatei des jeweilige Formats entspricht,
# # und so über die Web oberfläche der Datenbank hochgeladen werden kann
# path_out <- "W:/R/Datamanagement-2021data-edit/data/" # defriniert den outpath für die Loggerdatei
# long_data <- T #  speichert die daten in R im "long-format"
# 

### Initialies funktion to run

combine_DeltaT_files <- function(path, plot_name, subplot_name, year = year(Sys.time()), LoggerExport = T, path_out = "W:/R/Datamanagement/data/", long_data =T){
  print(c(plot_name, subplot_name))
  
  abbr.plot <- substring(plot_name, 1,2)
  abbr.sub <- substring(subplot_name, 1,2)
  #get all csv-file paths from directory
  l.paths <- list.files(path = path, pattern = "*.dat", full.names = T)
  if (length(l.paths)== 0){
    print("No Data found in directory or directory notexisting")
    stop()
  }
  #gather data form files, use readDeltaT from LoggerImports
  #to make sure Sensors have identical and consistent colomns names
  dat <- l.paths %>% map_df( ~ readDeltaT(.))
  #-----------------------------------------------------------------------------------
  ### Check Data
  # check time and other column classes
  # str(dat)
  # check data ranges and nas
  # summary(dat)
  # 1. check duplicated entries
  dup_check <- sum(duplicated(dat$Datum))
  if (dup_check != 0){
    dup_dat <- dat[duplicated(dat$Datum),]
  # we could do possible mor with these dublicated entries
  }

  # class(gaps_alt$Dat_diff)
  # as.difftime(gaps_alt$Dat_diff, format= "%H:%M")
  # as.numeric(gaps_alt$Dat_diff, units = "days")
  # format(gaps_alt$Dat_diff)
  # 3. check for right date-time format und tz
  # tz(dat$Datum);  class(dat$Datum); range(dat$Datum)
  # dat %>%  filter( Datum >= "2021-01-01 00:00:00 UTC")
  # sum(duplicated(dat$Datum))
  # 4. check data consistency
  #table(dat$Kanäle)
  #--------------------------------------------------------------------------------------
  # edit data, date time format, kick duplicated entries and filter for the choosen year
  dat1 <- 
    dat %>% arrange(Datum) %>%
    distinct(Datum, .keep_all= T) %>%
    #filter(!duplicated(dat$Datum)) %>%
    filter(year(Datum) >= year) %>% 
    filter(year(Datum) < year+1)
  
  
  #create LÜCKE
  #dat <- dat[-c(3000:4200),]
  # 2. check for missing data
  gaps <- check_for_ts_gaps(ts= dat1$Datum, max_diff= 24*60, list =F)
  print(gaps)
  
  

  print(paste("Es wurden Daten vom" , range(dat1$Datum, na.rm=T)[1], "bis zum", range(dat1$Datum, na.rm=T)[2], "gefunden und zusammengefasst"))

  # l <- unique(date(dat1$Datum))
  # t <- seq.Date(from = ymd(paste0(year, "-01-01")), to = ymd(paste0(year, "-12-31")), by = 1)
  # print(paste(" Von", length(t), "Tagen im Jahr", year," wurden", sum(l == t), "aufeinanderfolgenden Tage zusammengefasst." ))# alle Tage vorhanden!
  
  # ### CREATE LOGGER EXPORT FILE
  # if (LoggerExport == T){
  #   # prepare data for export in Logger-format
  #   dat_exp <- dat1 %>% 
  #     mutate(Datum = format(dat1$Datum, format = "%d.%m.%Y %H:%M")) %>% 
  #     mutate(No = seq(1:nrow(.))) %>% select(No, everything())
  #   # Export data
  #   # erstelle ein zusammengefasste tabelle f?r das jeweilige jahr
  #   writeLines("Logger: #D3000C 'Esslingen_Fi_FVA_1' - USP_EXP2 - (CGI) Expander for GP5W - (V2.60, Mai 12 2013)", paste0(path_out, abbr.plot, "_Level2",abbr.sub, "_DeltaT__", year,"_combine.csv"))
  #   suppressWarnings(write.table(dat_exp, file=paste0(path_out, abbr.plot, "_Level2",abbr.sub, "_DeltaT__", year,"_combine.csv"), sep = ";", dec=",", col.names = TRUE, append= TRUE, quote = F, row.names = F, na = ""))
  #   print(paste( "Es wurde eine Logger-Combi-Datei datei für das Jahr", year, "erstellt und unter", path_out, "gespeichert."))
  # }
  # 
  ### CREATE LOGGER EXPORT FILE
  if (LoggerExport == T){
    # prepare data for export in Logger-format
    dat_exp <- dat1 %>% 
      mutate(Datum = format(dat1$Datum, format = "%d.%m. %H:%M"))
     
    
    # Export data
    # erstelle ein zusammengefasste tabelle f?r das jeweilige jahr
 
    write.fwf2(dat_exp, file=paste0(path_out, abbr.plot, "_Level2",abbr.sub, "_DeltaT__", year,"_combine.dat"), year=year)
    print(paste( "Es wurde eine Logger-Combi-Datei datei für das Jahr", year, "erstellt und unter", path_out, "gespeichert."))
  }
  
  ### CREATE R DataFrame for further use
  if (long_data == T){
    # prepare data in R-Format
    dat_long <- dat1 %>% pivot_longer(. , cols= -Datum,  names_to = "variable", values_to = "value") %>% 
      mutate(Plot = plot_name, SubPlot = subplot_name) %>% 
      select(Plot, SubPlot, Datum, variable, value)
    return(dat_long)
  }
  else(
    return(dat1)
  )
  
 
  
  
}# end of function+

#testing funktion
#dat <- combine_DeltaT_files(path=path, plot_name = plot_name, subplot_name = subplot_name, year = year, LoggerExport = LoggerExport, long_data = long_data, path_out = path_out)
