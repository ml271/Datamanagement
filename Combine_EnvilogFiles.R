########################################################################
######################   Combine ESSLIINGEN FICHTE ENVILOG #################################
########################################################################
# autor: Marvin Lorff
# date: 18.01.2021


'Wichtig! BITTE LESEN'
# Dieses script fasst die Envilog Daten aus Esslingen zusammen!
# Es funktioniert NUR wenn noch KEINE zusammengefasste Datei ertsellt wurde!
# gegebenenfals muss die die Datei z.B. ES_Level2FI_Envilog_2021_combine.csv
# zuerst gel?scht werden, bevoir das script korrekt ausgef?hrt werden kann.
# oder Zeile 36 ( # tbl <- tbl[-1]) einkommentieren um die Datei auszulie?en


# Zum einlesen in die Level2 Datenbank, bitte folgendes beachten:
# Um das Datenformat aus den Loggern bei zu behalten muss nach dem die Datei erstellt wurde 
# in die erste Zeile noch die Logger informationen reinkopiert werden.
# Copie&paste: Logger: #D3000C 'Esslingen_Fi_FVA_1' - USP_EXP2 - (CGI) Expander for GP5W - (V2.60, Mai 12 2013)


# w?hle das jahr das zusammengefasst werden soll
vch.Year <- 2021


# load functions and libraries
comtodot <- function(x) {sub(",",".",x)} 
countna <- function(x) {sum(is.na(x))}
library(tidyverse)
library(lubridate)

#set directory to the Esslingen (ENVILOG) and the choosen Year
path <- paste0("O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Fichte_envilog/", vch.Year)
load("readEnvilog.R")
#-----------------------------------------------------------------------------------
#load all csv-file from directory
tbl <- list.files(path = path, pattern = "*.csv")
# tbl <- tbl[-1]
tbl <-  tbl %>%  map_df( ~read.csv2( ., skip=1,
                                     check.names=FALSE, na.strings = "(NOREPLY)",
                                     colClasses = c(rep("character",2), rep("numeric", 16), rep("character", 2), rep("numeric", 3))
                        )) 
#check columns format
str(tbl)

#change format and comma to point in columns that were not load correctly
tbl$'#17:?C KL:9:ID(60)' <- round(as.numeric(comtodot(tbl$'#17:?C KL:9:ID(60)')),2)
tbl$'#18:pF KL:(do.):ID(60)' <- round(as.numeric(comtodot(tbl$'#18:pF KL:(do.):ID(60)')),2)

# edit data, date time format, kick dublicated entries and filter for the choosen Year
tbl1 <- tbl %>%
  mutate(Time = dmy_hms(tbl$Time, truncated = 1)) %>%
  arrange(Time) %>% distinct(Time, .keep_all= T) %>%
  mutate(No = as.numeric(No)) %>% 
  filter(year(Time) >= vch.Year) %>% 
  filter(year(Time) < vch.Year+1) %>%  select(-No)


#test
countna(tbl1$Time)
range(tbl1$Time, na.rm=T)
head(tbl1)
str(tbl1)
countna(tbl1$No)
l <- unique(date(tbl1$Time))
t <- seq.Date(from = ymd(paste0(vch.Year, "-01-01")), to = ymd(paste0(vch.Year, "-12-31")), by = 1)
l == t # alle Tage vorhanden!


# prepare data for export
tbl2 <- tbl1 %>% 
  mutate(Time = format(tbl1$Time, format = "%d.%m.%Y %H:%M")) %>% 
  distinct() %>% 
  mutate(No = seq(1:nrow(.))) %>% select(No, everything())

#test
names(tbl1)
str(tbl2)
head(tbl2)
tail(tbl2)

# Export data
# erstelle ein zusammengefasste tabelle f?r das jeweilige jahr
write.csv2(tbl2, file=paste0("ES_Level2FI_Envilog__", vch.Year,"_combine.csv"), quote = F, row.names = F, na = "")

# Zum einlesen in die Level2 Datenbank, bitte folgendes beachten:
# Um das Datenformat aus den Loggern bei zu behalten muss nach dem die Datei erstellt wurde 
# in die erste Zeile noch die Logger informationen reinkopiert werden.
# Copie&paste: Logger: #D3000C 'Esslingen_Fi_FVA_1' - USP_EXP2 - (CGI) Expander for GP5W - (V2.60, Mai 12 2013)
