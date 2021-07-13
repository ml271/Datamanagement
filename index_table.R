#Import Librarys
library(dplyr)  # v 1.0.1 used for Datamanipulation
library(dbplyr) # v 1.4.4 used for Datamanipulation
library(odbc)   # v 1.2.3 used for Database connections
#
# Set working directory
#       Muss für die "produktion" an den Server angeglichen werden
setwd("W:/R")
#
conn.level2db <- DBI::dbConnect(odbc::odbc(),
                                          driver            = "SQL Server",
                                          server            = "fvafrsd-2v\\PRODUCTION",
                                          database          = "messdatendb",
                                          uid               = "mddb_lesen",
                                          pwd               = "md83_r43D",
                                          TrustedConnection = TRUE,
                                          encoding          = "latin1")


get_index_table <- function(DBCONNECTION_PRODUCTION){
    # Einladen der "nötigen" Tables
#
#     TMP_vgv_st_stamm_multi <- dbGetQuery(
#         DBCONNECTION_PRODUCTION,
#         paste("SELECT * FROM messdatendb.mddb_gv.vgv_st_stamm_multi"))

    TMP_KMD_LG_MESSGROESSE <- dbGetQuery(
        DBCONNECTION_PRODUCTION,
        paste("SELECT * FROM messdatendb.mddb_md.kmd_lg_messgroesse"))

        TMP_KMD_MESSGROESSE    <- dbGetQuery(
        DBCONNECTION_PRODUCTION,
        paste("SELECT * FROM messdatendb.mddb_md.kmd_messgroesse"))

        TMP_KMD_EINHEIT        <- dbGetQuery(
        DBCONNECTION_PRODUCTION,
        paste("SELECT * FROM messdatendb.mddb_md.kmd_einheit"))

    TMP_KMD_LUECKENERSATZ  <- dbGetQuery(
        DBCONNECTION_PRODUCTION,
        paste("SELECT * FROM messdatendb.mddb_md.kmd_lueckenersatz"))
    #
    # TMP_TGV_LG_STAMM       <- dbGetQuery(
    #     DBCONNECTION_PRODUCTION,
    #     paste("SELECT * FROM messdatendb.mddb_gv.tgv_lg_stamm"))
    #
        TMP_TGV_SE_STAMM       <- dbGetQuery(
        DBCONNECTION_PRODUCTION,
        paste("SELECT * FROM messdatendb.mddb_gv.tgv_se_stamm"))

        TMP_VGV_LG_ST          <- dbGetQuery(
        DBCONNECTION_PRODUCTION,
        paste("SELECT * FROM messdatendb.mddb_gv.Vgv_lg_st"))

        TMP_TGV_LG_KONF_DATEN  <- dbGetQuery(
        DBCONNECTION_PRODUCTION,
        paste("SELECT * FROM messdatendb.mddb_gv.tgv_lg_konf_daten"))

        TMP_KGV_LOGGERTYP      <- dbGetQuery(
        DBCONNECTION_PRODUCTION,
        paste("SELECT * FROM messdatendb.mddb_gv.kgv_loggertyp"))

    #
    #-------------------------------------------------------------------------------
    #
    # Gesamt table

    # Ermitteln der notwendigen länge
    #
    TMP <- nrow(TMP_TGV_LG_KONF_DATEN)
    TMP <- rep(NA,TMP)
    #
    TMP_LK_ID               = TMP_TGV_LG_KONF_DATEN$lk_id
    #
    # Daten aus vgv_lg-stamm
    #
    TMP_LG_ID               = TMP
    TMP_LG_BEZEICHNUNG      = TMP
    TMP_LOGGERTYPE_CODE     = TMP
    TMP_LOGGER_SERIENNUMMER = TMP
    TMP_GUELTIG_VON         = TMP
    TMP_GUELTIG_BIS         = TMP
    #
    TMP_ST_ID               = TMP
    TMP_ST_BEZEICHNUNG      = TMP
    #
    for (A in 1:length(TMP_VGV_LG_ST$lk_id)) {
        TMP_LG_ID[which(TMP_LK_ID               == TMP_VGV_LG_ST$lk_id[A])] =  TMP_VGV_LG_ST$lg_id[A]
        TMP_LG_BEZEICHNUNG[which(TMP_LK_ID      == TMP_VGV_LG_ST$lk_id[A])] =  TMP_VGV_LG_ST$lg_bezeichnung[A]
        TMP_LOGGERTYPE_CODE[which(TMP_LK_ID     == TMP_VGV_LG_ST$lk_id[A])] =  TMP_VGV_LG_ST$loggertyp_code[A]
        TMP_LOGGER_SERIENNUMMER[which(TMP_LK_ID == TMP_VGV_LG_ST$lk_id[A])] =  TMP_VGV_LG_ST$seriennr[A]
        TMP_ST_ID[which(TMP_LK_ID               == TMP_VGV_LG_ST$lk_id[A])] =  TMP_VGV_LG_ST$st_id[A]
        TMP_ST_BEZEICHNUNG[which(TMP_LK_ID      == TMP_VGV_LG_ST$lk_id[A])] =  TMP_VGV_LG_ST$st_bezeichnung[A]
        TMP_GUELTIG_VON[which(TMP_LK_ID         == TMP_VGV_LG_ST$lk_id[A])] =  as.numeric(TMP_VGV_LG_ST$gueltig_von[A])
        TMP_GUELTIG_BIS[which(TMP_LK_ID         == TMP_VGV_LG_ST$lk_id[A])] =  as.numeric(TMP_VGV_LG_ST$gueltig_bis[A])
    }
    A = 0
    #
    TMP_LOGGERTYPE = TMP
    for (A in 1:length(TMP_KGV_LOGGERTYP$loggertyp_code)) {
        TMP_LOGGERTYPE[which(TMP_LOGGERTYPE_CODE == TMP_KGV_LOGGERTYP$loggertyp_code[A])] = TMP_KGV_LOGGERTYP$loggertyp_bez[A]
    }

    #
    TMP_SE_ID                          = TMP_TGV_LG_KONF_DATEN$se_id
    TMP_SE_BEZEICHNUNG                 = TMP
    TMP_SE_TYP_SNR                     = TMP
    #TMP_SE_TYP                        = TMP se_stamm
    #TMP_SE_SNR                        = TMP se_stamm
    for (A in 1:length(TMP_TGV_SE_STAMM$se_id)) {
        TMP_SE_BEZEICHNUNG[which(TMP_SE_ID == TMP_TGV_SE_STAMM$se_id[A])] = TMP_TGV_SE_STAMM$se_bezeichnung[A]
        TMP_SE_TYP_SNR[which(TMP_SE_ID     == TMP_TGV_SE_STAMM$se_id[A])] = TMP_TGV_SE_STAMM$se_typ_snr[A]
        #TMP_SE_TYP                        = TMP se_stamm
        #TMP_SE_SNR                        = TMP se_stamm
    }
    A                                  = 0
    #
    TMP_LG_MESSGROESSE_ID              = TMP_TGV_LG_KONF_DATEN$lg_messgroesse_id
    TMP_LG_MESSGROESSE                 = TMP
    TMP_LG_MG_NAME                     = TMP
    TMP_LG_SPALTENTITEL                = TMP_TGV_LG_KONF_DATEN$lg_spaltentitel
    TMP_MESSGROESSE_ID                 = TMP
    #
    for (A in 1:length(TMP_KMD_LG_MESSGROESSE$lg_messgroesse_id)) {
        TMP_LG_MESSGROESSE[which(TMP_LG_MESSGROESSE_ID == TMP_KMD_LG_MESSGROESSE$lg_messgroesse_id[A])] = TMP_KMD_LG_MESSGROESSE$lg_messgroesse[A]
        TMP_LG_MG_NAME[which(TMP_LG_MESSGROESSE_ID     == TMP_KMD_LG_MESSGROESSE$lg_messgroesse_id[A])] = TMP_KMD_LG_MESSGROESSE$lg_mg_name[A]
        TMP_MESSGROESSE_ID[which(TMP_LG_MESSGROESSE_ID == TMP_KMD_LG_MESSGROESSE$lg_messgroesse_id[A])] = TMP_KMD_LG_MESSGROESSE$messgroesse_id[A]
    }
    A                         = 0
    #
    TMP_MESSGROESSE           = TMP
    TMP_MG_NAME               = TMP
    TMP_EINHEIT_ID            = TMP_TGV_LG_KONF_DATEN$einheit_id
    TMP_UNTERGRENZE           = TMP
    TMP_OBERGRENZE            = TMP
    TMP_LUECKENERSATZ_CODE    = TMP
    TMP_LUECKENERSATZ_MINUTEN = TMP
    TMP_AGG_PROZENT           = TMP
    #
    for (A in 1:length(TMP_KMD_MESSGROESSE$messgroesse_id)) {
        TMP_MESSGROESSE[which(TMP_MESSGROESSE_ID           == TMP_KMD_MESSGROESSE$messgroesse_id[A])] = TMP_KMD_MESSGROESSE$messgroesse[A]
        TMP_MG_NAME[which(TMP_MESSGROESSE_ID               == TMP_KMD_MESSGROESSE$messgroesse_id[A])] = TMP_KMD_MESSGROESSE$mg_name[A]
        TMP_UNTERGRENZE[which(TMP_MESSGROESSE_ID           == TMP_KMD_MESSGROESSE$messgroesse_id[A])] = TMP_KMD_MESSGROESSE$untergrenze[A]
        TMP_OBERGRENZE[which(TMP_MESSGROESSE_ID            == TMP_KMD_MESSGROESSE$messgroesse_id[A])] = TMP_KMD_MESSGROESSE$obergrenze[A]
        TMP_LUECKENERSATZ_CODE[which(TMP_MESSGROESSE_ID    == TMP_KMD_MESSGROESSE$messgroesse_id[A])] = TMP_KMD_MESSGROESSE$lueckenersatz_code[A]
        TMP_LUECKENERSATZ_MINUTEN[which(TMP_MESSGROESSE_ID == TMP_KMD_MESSGROESSE$messgroesse_id[A])] = TMP_KMD_MESSGROESSE$lueckenersatz_minuten[A]
        TMP_AGG_PROZENT[which(TMP_MESSGROESSE_ID           == TMP_KMD_MESSGROESSE$messgroesse_id[A])] = TMP_KMD_MESSGROESSE$agg_prozent[A]
    }
    A = 0
    #
    TMP_LUECKENERSATZ_BEZ = TMP
    for (A in 1:length(TMP_KMD_LUECKENERSATZ$lueckenersatz_code)) {
        TMP_LUECKENERSATZ_BEZ[which(TMP_LUECKENERSATZ_CODE == TMP_KMD_LUECKENERSATZ$lueckenersatz_code[A])] = TMP_KMD_LUECKENERSATZ$lueckenersatz_bez[A]
    }
    A = 0
    #
    TMP_EINHEIT      = TMP
    TMP_EINHEIT_NAME = TMP
    for (A in 1:length(TMP_KMD_EINHEIT$einheit_id)) {
        TMP_EINHEIT[which(TMP_EINHEIT_ID      == TMP_KMD_EINHEIT$einheit_id[A])] = TMP_KMD_EINHEIT$einheit[A]
        TMP_EINHEIT_NAME[which(TMP_EINHEIT_ID == TMP_KMD_EINHEIT$einheit_id[A])] = TMP_KMD_EINHEIT$einheit_name[A]
    }
    A = 0
    #
    #--------------------------------------------
    # Einrichten des dataframe
    INDEX_STATIONS_SENSORS <- data.frame(
        LK_ID                 = TMP,
        ST_ID                 = TMP,
        ST_BEZEICHNUNG        = TMP,
        LG_ID                 = TMP,
        LG_BEZEICHNUNG        = TMP,
        LOGGERTYPE_CODE       = TMP,
        LOGGERTYPE_BEZ        = TMP,
        LOGGER_SERIENNUMMER   = TMP,
        GUELTIG_VON           = TMP,
        GUELTIG_BIS           = TMP,
        SE_ID                 = TMP,
        SE_BEZEICHNUNG        = TMP,
        SE_TYP_SNR            = TMP,
        #SE_TYP se_stamm      = TMP,
        #SE_SNR se_stamm      = TMP,
        LG_MESSGROESSE_ID     = TMP,
        LG_MESSGROESSE        = TMP,
        LG_MG_NAME            = TMP,
        LG_SPALTENTITEL       = TMP,
        MESSGROESSE_ID        = TMP,
        MESSGROESSE           = TMP,
        MG_NAME               = TMP,
        EINHEIT_ID            = TMP,
        EINHEIT               = TMP,
        EINHEIT_NAME          = TMP,
        UNTERGRENZE           = TMP,
        OBERGRENZE            = TMP,
        LUECKENERSATZ_CODE    = TMP,
        LUECKENERSATZ_BEZ     = TMP,
        LUECKENERSATZ_MINUTEN = TMP,
        AGG_PROZENT           = TMP
    )
    INDEX_STATIONS_SENSORS$LK_ID                 = TMP_LK_ID
    INDEX_STATIONS_SENSORS$LG_ID                 = TMP_LG_ID
    INDEX_STATIONS_SENSORS$LG_BEZEICHNUNG        = TMP_LG_BEZEICHNUNG
    INDEX_STATIONS_SENSORS$LOGGERTYPE_CODE       = TMP_LOGGERTYPE_CODE
    INDEX_STATIONS_SENSORS$LOGGERTYPE_BEZ        = TMP_LOGGERTYPE
    INDEX_STATIONS_SENSORS$LOGGER_SERIENNUMMER   = TMP_LOGGER_SERIENNUMMER
    INDEX_STATIONS_SENSORS$GUELTIG_VON           = TMP_GUELTIG_VON
    INDEX_STATIONS_SENSORS$GUELTIG_BIS           = TMP_GUELTIG_BIS
    INDEX_STATIONS_SENSORS$ST_ID                 = TMP_ST_ID
    INDEX_STATIONS_SENSORS$ST_BEZEICHNUNG        = TMP_ST_BEZEICHNUNG
    INDEX_STATIONS_SENSORS$SE_ID                 = TMP_SE_ID
    INDEX_STATIONS_SENSORS$SE_BEZEICHNUNG        = TMP_SE_BEZEICHNUNG
    INDEX_STATIONS_SENSORS$SE_TYP_SNR            = TMP_SE_TYP_SNR
    #INDEX_STATIONS_SENSORS$SE_TYP               = TMP_SE_TYP
    #INDEX_STATIONS_SENSORS$SE_SNR               = TMP_SE_SNR
    INDEX_STATIONS_SENSORS$LG_MESSGROESSE_ID     = TMP_LG_MESSGROESSE_ID
    INDEX_STATIONS_SENSORS$LG_MESSGROESSE        = TMP_LG_MESSGROESSE
    INDEX_STATIONS_SENSORS$LG_MG_NAME            = TMP_LG_MG_NAME
    INDEX_STATIONS_SENSORS$LG_SPALTENTITEL       = TMP_LG_SPALTENTITEL
    INDEX_STATIONS_SENSORS$MESSGROESSE_ID        = TMP_MESSGROESSE_ID
    INDEX_STATIONS_SENSORS$MESSGROESSE           = TMP_MESSGROESSE
    INDEX_STATIONS_SENSORS$MG_NAME               = TMP_MG_NAME
    INDEX_STATIONS_SENSORS$EINHEIT_ID            = TMP_EINHEIT_ID
    INDEX_STATIONS_SENSORS$EINHEIT               = TMP_EINHEIT
    INDEX_STATIONS_SENSORS$EINHEIT_NAME          = TMP_EINHEIT_NAME
    INDEX_STATIONS_SENSORS$UNTERGRENZE           = TMP_UNTERGRENZE
    INDEX_STATIONS_SENSORS$OBERGRENZE            = TMP_OBERGRENZE
    INDEX_STATIONS_SENSORS$LUECKENERSATZ_CODE    = TMP_LUECKENERSATZ_CODE
    INDEX_STATIONS_SENSORS$LUECKENERSATZ_MINUTEN = TMP_LUECKENERSATZ_MINUTEN
    INDEX_STATIONS_SENSORS$AGG_PROZENT           = TMP_AGG_PROZENT
    INDEX_STATIONS_SENSORS$LUECKENERSATZ_BEZ     = TMP_LUECKENERSATZ_BEZ


return(INDEX_STATIONS_SENSORS)
}



    #-------------------------------------------------------------------------------
