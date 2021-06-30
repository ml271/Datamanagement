########################################################################
######################   Main Datamanagement #################################
########################################################################
# autor: Marvin Lorff
# date: 28.01.2021
# version: 02.02

# load functions and libraries
library(tidyverse)
library(lubridate)
source("Combine_EnvilogFiles.R")
source("Combine_DeltaTFiles.R")
source("Combine_AccessConvent.R")
#-----------------------------------------------------------------------------------
combi_ess_2021 <- combine_Envilog_files(path="O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Fichte_envilog/2021",
                                        plot_name ="Esslingen", subplot_name = "Fichte",year=2021,
                                        LoggerExport = T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/")

combi_och_2021 <- combine_Envilog_files("O:/PROJEKT/NIEDER/LOGGER/OCHS/Ochsenhausen_Fichte_ungedüngt_envilog/2021",
                                        plot_name = "Ochsenhausen", subplot_name = "Fichte", year=2021,
                                        LoggerExport = T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/")

combi_rot_2021 <- combine_Envilog_files("O:/PROJEKT/NIEDER/LOGGER/ROTENFEL/Rotenfels_Fichte_Envilog/2021",
                                        plot_name = "Rotenfels", subplot_name = "Fichte", year=2021,
                                        LoggerExport = T, long_data = T, path_out ="W:/R/Datamanagement-2021data-edit/data/")

#-----------------------------------------------------------------------------------# 
combi_ess_bu_2021 <- combine_DeltaT_files(path="O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Buche_DeltaT/backup.dat/2021",
                                          plot_name ="Esslingen", subplot_name = "Buche", year= 2021,
                                          LoggerExport = T, long_data = T, path_out = "W:/R/Datamanagement-2021data-edit/data/")

combi_ess_fi_2021 <- combine_DeltaT_files(path="O:/PROJEKT/NIEDER/LOGGER/ESSLINGN/FVA/Esslingen_Fichte_DeltaT/2021",
                                          plot_name ="Esslingen", subplot_name = "Fichte", year= 2021,
                                          LoggerExport = T, long_data = T, path_out = "W:/R/Datamanagement-2021data-edit/data/")

combi_rot_2021    <- combine_DeltaT_files("O:/PROJEKT/NIEDER/LOGGER/ROTENFEL/Rotenfels_Fichte_DeltaT_neu/backup.dat/2021",
                                          plot_name = "Rotenfels", subplot_name = "Fichte", year=2021,
                                          LoggerExport = T, long_data = T, path_out = "W:/R/Datamanagement-2021data-edit/data/")

combi_rot_2021    <- combine_DeltaT_files("O:/PROJEKT/NIEDER/LOGGER/OCHS/Ochsenhausen_Fichte_gedüngt_Delta_T/2021",
                                          plot_name = "Ochsenhausen", subplot_name = "Fichte", year= 2021,
                                          LoggerExport = T, long_data = T, path_out = "W:/R/Datamanagement-2021data-edit/data/")

#-----------------------------------------------------------------------------------
co_dl1_2001 <- extract_Access_data(dl_table = "DL1_BTA1", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

co_dl2_2001 <- extract_Access_data(dl_table = "DL2_BFI2", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

co_dl3_2001 <- extract_Access_data(dl_table = "DL3_WFI2", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

co_dl4_2001 <- extract_Access_data(dl_table = "DL4_WFI4", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

#co_dl5_2001 <- extract_Access_data(dl_table = "DL5_WFEN", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")

co_dl6_2001 <- extract_Access_data(dl_table = "DL6_BBU5", year=2021, with_flags= F, path_out= "W:/R/Datamanagement-2021data-edit/data/")
#-----------------------------------------------------------------------------------