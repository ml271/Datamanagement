library(lubridate)
library(dplyr)
library(RODBC)
library(purrr)
library(viridis)
library(xlsx)
"%ni%" <- Negate("%in%")

####### Get data from händische Messungen for P-Q Beziehung ######
setwd("O:/PROJEKT/CONVENT/WEHRDAT/Keller/")
path.db <- "pegelsonde"
## open connection
rodbc.connect <- odbcConnectAccess(path.db, DBMSencoding = "latin1")
SQLQuery_dl2 <- paste0("SELECT * FROM Pegeleichung")
## get data
p.eich <- sqlQuery(rodbc.connect, SQLQuery_dl2, stringsAsFactors = FALSE ,as.is=T) %>%
  mutate(Date_Time = ymd_hms(Dat_Zeit)) %>%
    arrange(Date_Time) %>%
    select(Date_Time, everything(), -Dat_Zeit) %>% collect()

tail(p.eich)
odbcClose(rodbc.connect)
# round time on 15 minutes
minute(p.eich$Date_Time) <- sapply(p.eich$Date_Time,FUN = function(x){return(round(minute(x)/15)*15)},simplify=T)

####### Calculate P-Q Beziehungen ######
qm <- lm(p.eich$Durchfluss ~ poly(p.eich$Pegelmessung,2,raw=T))
summary(qm)
predict <- data.frame("P" = seq(10, 30, length.out = length(p.eich$Pegelmessung)))
predict$Q = qm$coefficient[3]*predict$P^2 + qm$coefficient[2]*predict$P + qm$coefficient[1]#y = 0.03633876*x^2 - 0.8015594*x + 4.425754

### plot P-Q
plot(p.eich$Durchfluss ~ p.eich$Pegelmessung, xlab= "Pegel [cm]", xlim=c(10,32), ylab="Abfluss [l/s]")
lines(predict$P, predict$Q, col = "red", lwd = 1)
mtext("P_Q Beziehung Pegelmessung (händische Messungen)", line=2, cex=1.2)
mtext(paste0("Y = ",round(qm$coefficient[3],4),"X^2 - ",round(qm$coefficient[2],4), "*X +", round(qm$coefficient[1],5)),side=3, line= 0.8)
#######----------------------------------------------------------------------###########



############################################################################################################################
####### calculate P_Q-Relation for Ott sonde
# get data from specific path
path <- "W:/Datenbank Level2/Import DB/Wehr/Ott_import/"
l.paths <- list.files(path = path, pattern = "*.csv", full.names = T)

#combine all ott data into df
ott_ges <- l.paths %>% map_df( ~ read.csv2(.,dec=",", as.is = T, na.strings = c("[5]", "[10]"))) %>%
    mutate(Date_Time = dmy_hm(Datum...Uhrzeit, truncated = 4)) %>%
    select(Date_Time, Water.level, everything(), -c(Datum...Uhrzeit)) %>%
    distinct(Date_Time, .keep_all = T) %>% arrange(Date_Time)

#round time in 15 intervals
minute(ott_ges$Date_Time) <- sapply(ott_ges$Date_Time,FUN = function(x){return(round(minute(x)/15)*15)},simplify=T)
#sum(minute(ott_ges$Date_Time))
# water .level in cm
# ott_ges$Water.level <- ott_ges$Water.level *100

# combine ott data with data form pegelmessungen
# p.eich contains all the daat from access DB ( alle händischen Durchflussmessungen)

head(ott_ges) ; head(p.eich)
ott_ges1 <- left_join(ott_ges, p.eich, by= "Date_Time") %>% arrange(Date_Time)
ott_ges1 %>%  filter( Date_Time == "2021-03-15 15:15:00")
#e xtract data to calc P-Q Relation
ott_eich <- ott_ges1[-which(is.na(ott_ges1$Durchfluss)),]

# Calculate P-Q BEziehung for Ott Sonde
plot(ott_eich$Durchfluss ~ ott_eich$Water.level)
qm1 <- lm(ott_eich$Durchfluss ~ poly(ott_eich$Water.level,2,raw=T))
water.l.range <- range(ott_eich$Water.level, na.rm=T)
summary(qm1)
predict1 <- data.frame("P" = seq(water.l.range[1],water.l.range[2], length.out = length(ott_eich$Durchfluss)))
predict1$Q = qm1$coefficient[3]*predict1$P^2 + qm1$coefficient[2]*predict1$P + qm1$coefficient[1]

# Plot P-Q Beziehung
plot(ott_eich$Durchfluss ~ ott_eich$Water.level, xlab= "Pegel [m]", ylab="Abfluss [l/s]",
     pch=16, col = viridis(length(year(ott_eich$Date_Time))))
lines(predict1$P, predict1$Q, col = "red", lwd = 1)
mtext("P_Q Beziehung Ott-Sonde", line=2, cex=1.2)
mtext(paste0("Y = ", round(qm1$coefficients[3],3), "*X^2 - ", round(qm1$coefficients[2],3)," *X +", round(qm1$coefficients[1],3)),side=3, line= 0.8)
#legend("bottomleft", legend = year(ott_eich$Date_Time), col =  viridis(length(year(ott_eich$Date_Time))))

# add discharge to ott_ges data
ott_ges1$Abfluss.Ott <- qm1$coefficients[3] * ott_ges1$Water.level^2 + qm1$coefficients[2] * ott_ges1$Water.level + qm1$coefficients[1]
ott_ges2 <- ott_ges1 %>% filter( Date_Time >= "2018-06-09 02:15:00 UTC") %>%
    relocate(Abfluss.Ott, .after = "TDS") %>%
    mutate( Datum = as.character(Date_Time, format = "%d.%m.%Y %H:%M")) %>%
    select(Datum, everything(), -c(Date_Time))

# head(ott_ges1 %>% filter( Date_Time >= "2018-06-09 02:15:00 UTC"))
# head(ott_ges1 %>% mutate(Datum = as.character(Date_Time, format = "%d.%m.%Y %H:%M")) )
# head(ott_ges2)
# ott_ges1[which(!is.na(ott_ges2$Durchfluss)),]
# ott_ges2[which(!is.na(ott_ges2$Durchfluss)),]
#### Export ott_ges data as csv


write.csv2(ott_ges2, file="W:/Datenbank Level2/Import DB/Wehr/ott_gesamt.csv", row.names = F, quote = F )
xlsx::write.xlsx2(ott_ges1, file="W:/Datenbank Level2/Import DB/Wehr/ott_gesamt.xlsx", row.names = F)
#-----------------------------------------------------------------------------------------------------------------------------
names(ott_ges)
ott_eich %>%  filter( Date_Time >= "2020-01-01")
plot(ott_ges$Abfluss.Ott ~ ott_ges$Date_Time, type="l")
plot(ott_ges$Abfluss.Ott[30000:50000] ~ ott_ges$Date_Time[30000:50000], type="l")
abline(h=0)
