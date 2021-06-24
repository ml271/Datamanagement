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
setwd("W:/R/Datamanagement")

# load functions and libraries
comtodot <- function(x) {sub(",",".",x)} 
countna <- function(x) {sum(is.na(x))}
library(tidyverse)
library(lubridate)
source("functions/readEnvilog.R")
source("functions/check_for_ts_gaps.R")

#set directory to the Esslingen (ENVILOG) and the choosen Year
path <- paste0("O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Fichte_envilog/", vch.Year)

#-----------------------------------------------------------------------------------
### LOADING DATA FROM PATH

#get all csv-file paths from directory
l.paths <- list.files(path = path, pattern = "*.csv", full.names = T)
#gather data form files, use readEnvilog to make sure Sensors have identical and consistent colomns names
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
gaps <- check_for_ts_gaps(ts= dat$Datum, max_diff= 24*60 )
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


# edit data, date time format, kick dublicated entries and filter for the choosen Year
dat1 <- dat %>%
  mutate(Time = dmy_hms(tbl$Time, truncated = 1)) %>%
  arrange(Time) %>% distinct(Time, .keep_all= T) %>%
  mutate(No = as.numeric(No)) %>% 
  filter(year(Time) >= vch.Year) %>% 
  filter(year(Time) < vch.Year+1) %>%  select(-No)

dat1 <- 
  dat %>% arrange(Datum) %>%
  distinct(Datum, .keep_all= T) %>%
  filter(!duplicated(dat$Datum)) %>%
  filter(year(Datum) >= vch.Year) %>% 
  filter(year(Datum) < vch.Year+1)

#test
countna(dat1$Datum)
range(dat1$Datum, na.rm=T)
head(dat1)
str(dat1)

l <- unique(date(dat1$Datum))
t <- seq.Date(from = ymd(paste0(vch.Year, "-01-01")), to = ymd(paste0(vch.Year, "-12-31")), by = 1)
l == t # alle Tage vorhanden!


# prepare data for export
dat2 <- dat1 %>% 
  mutate(Datum = format(dat1$Datum, format = "%d.%m.%Y %H:%M")) %>% 
  distinct() %>% 
  mutate(No = seq(1:nrow(.))) %>% select(No, everything())

#test
names(dat2)
str(dat2)
head(dat2)
tail(dat2)

# TODO: convert data into long format
# TODO: load all plots/Subplots at once

# Export data
# erstelle ein zusammengefasste tabelle f?r das jeweilige jahr
writeLines("Logger: #D3000C 'Esslingen_Fi_FVA_1' - USP_EXP2 - (CGI) Expander for GP5W - (V2.60, Mai 12 2013)", paste0("data/ES_Level2FI_Envilog__", vch.Year,"_combine.csv"))
write.table(dat2, file=paste0("data/ES_Level2FI_Envilog__", vch.Year,"_combine.csv"), sep = ";", dec=",", col.names = TRUE, append= TRUE, quote = F, row.names = F, na = "")

# Zum einlesen in die Level2 Datenbank, bitte folgendes beachten:
# Um das Datenformat aus den Loggern bei zu behalten muss nach dem die Datei erstellt wurde 
# in die erste Zeile noch die Logger informationen reinkopiert werden.
# Copie&paste: Logger: #D3000C 'Esslingen_Fi_FVA_1' - USP_EXP2 - (CGI) Expander for GP5W - (V2.60, Mai 12 2013)
