########################################################################
######################   Main Datamanagement #################################
########################################################################
# autor: Marvin Lorff
# date: 28.01.2021
# version: 02.02

# TODO: ....


# load functions and libraries
library(tidyverse)
library(lubridate)
source("Combine_EnvilogFiles.R")
source("Combine_DeltaTFiles.R")
source("Combine_AccessConvent.R")
source("Load_ADLMdata.R")
#-----------------------------------------------------------------------------------
ess_fi_Envilog_2021 <- combine_Envilog_files(path="O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Fichte_envilog/2021",
                                        plot_name ="Esslingen", subplot_name = "Fichte",year=2021,
                                        LoggerExport = T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/")

och_fi_Envilog_2021 <- combine_Envilog_files("O:/PROJEKT/NIEDER/LOGGER/OCHS/Ochsenhausen_Fichte_ungedüngt_envilog/2021",
                                        plot_name = "Ochsenhausen", subplot_name = "Fichte", year=2021,
                                        LoggerExport = T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/")

rot_fi_Envilog_2021 <- combine_Envilog_files("O:/PROJEKT/NIEDER/LOGGER/ROTENFEL/Rotenfels_Fichte_Envilog/2021",
                                        plot_name = "Rotenfels", subplot_name = "Fichte", year=2021,
                                        LoggerExport = T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/")

#-----------------------------------------------------------------------------------# 
ess_bu_DeltaT_2021 <- combine_DeltaT_files(path="O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Buche_DeltaT/backup.dat/2021",
                                          plot_name ="Esslingen", subplot_name = "Buche", year= 2021,
                                          LoggerExport = T, long_data = T, path_out = "W:/R/Datamanagement-2021data-edit/data/")

ess_fi_DeltaT_2021 <- combine_DeltaT_files(path="O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Fichte_DeltaT/2021",
                                          plot_name ="Esslingen", subplot_name = "Fichte", year= 2021,
                                          LoggerExport = T, long_data = T, path_out = "W:/R/Datamanagement-2021data-edit/data/")

rot_fi_DeltaT_2021    <- combine_DeltaT_files("O:/PROJEKT/NIEDER/LOGGER/ROTENFEL/Rotenfels_Fichte_DeltaT_neu/backup.dat/2021",
                                          plot_name = "Rotenfels", subplot_name = "Fichte", year=2021,
                                          LoggerExport = T, long_data = T, path_out = "W:/R/Datamanagement-2021data-edit/data/")

oc_fidue_DeltaT_2021    <- combine_DeltaT_files("O:/PROJEKT/NIEDER/LOGGER/OCHS/Ochsenhausen_Fichte_gedüngt_Delta_T/2021",
                                          plot_name = "Ochsenhausen", subplot_name = "Fichte", year= 2021,
                                          LoggerExport = T, long_data = T, path_out = "W:/R/Datamanagement-2021data-edit/data/")

#-----------------------------------------------------------------------------------
co_dl1_2021 <- extract_Access_data(dl_table = "DL1_BTA1", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

co_dl2_2021 <- extract_Access_data(dl_table = "DL2_BFI2", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

co_dl3_2021 <- extract_Access_data(dl_table = "DL3_WFI2", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

co_dl4_2021 <- extract_Access_data(dl_table = "DL4_WFI4", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

#co_dl5_2001 <- extract_Access_data(dl_table = "DL5_WFEN", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

co_dl6_2021 <- extract_Access_data(dl_table = "DL6_BBU5", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")
#-----------------------------------------------------------------------------------
# Possible entries for ADLM data requests
BLACKLIST <- read.csv(
  paste("./BLACKLIST_StandorteADLM.csv", sep = ""),
  sep = ";",
  check.names = TRUE,
  header = TRUE,
  encoding = "UTF-8"
)  

#Heidelberg Buche 1 scheint nicht exdportierbar aus der ADLM DB desswegn wird sie hier mit auf die Blacklist geschireben
BLACKLIST$Blacklist[which(BLACKLIST$Eintrag_ADLM == "Heidelberg_Bu 1")] <- "y"
WHITELIST<-  BLACKLIST %>% filter(Blacklist == "n" ) %>%
  select(Eintrag_ADLM, Station_Flaeche)
adlm_plot_list <- strsplit(WHITELIST$Station_Flaeche, split = "_", fixed= T)


for (i in 1: length(adlm_plot_list)){
  abbr.plot <- substring(adlm_plot_list[[i]][1], 1,2)
  abbr.sub <- substring(adlm_plot_list[[i]][2], 1,2)
  #l.adlm[[i]] <- load_ADLM_data(year = 2021, plot_name = adlm_plot_list[[i]][1], subplot_name = adlm_plot_list[[i]][2], LoggerExport = T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/ADLM/")
  temp <- load_ADLM_data(year = 2021, plot_name = adlm_plot_list[[i]][1], subplot_name = adlm_plot_list[[i]][2], LoggerExport = T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/ADLM/")
  assign(paste0(abbr.plot, "_",abbr.sub, "_ADLM_2021"), temp)
}

####################################################################################
rm(BLACKLIST, WHITELIST, adlm_plot_list, temp, abbr.plot, abbr.sub, ExportLogger, long_data, i, path_out, plot_name, subplot_name, year)
save.image(file = "my2021data.Rdata")
#save(list = ls(all.names = TRUE,pattern="co_dl"), file = "my2021data.RData", envir = .GlobalEnv )
#system('shutdown -s')
load("my2021data.Rdata")
rm(list=ls(all.names = T))

