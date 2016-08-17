# +----------------------------------------------------------------+
# | Big Data Praktikum, SoSe 2016                                  |       
# | "Temporale Analyse von News-Daten und Kursentwicklung"         |
# | J. Geisler und T. Tanck                                        |
# + ---------------------------------------------------------------+

# connecting to SparkR
# --------------------

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/apache-spark/")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "local[*]", sparkEnvir = list(spark.driver.memory="2g"))

sqlContext <- sparkRSQL.init(sc)

require(sqldf)
require(data.table)

# loading data sets
# -----------------

getwd()

bcData <- read.csv("BCHAIN-NTRAN.csv", header = TRUE, sep = ",") #loading dataset 'number of bitcoin transactions per day'

baverageUsd <- read.csv("BAVERAGE-USD.csv", header = TRUE, sep = ",") #loading bitcoin course data


tweets <- read.csv("tweetsPerDay.csv", header = TRUE, sep = ",") #loading dataset 'number of #bitcoin-tweets per day'
tweets <- tweets[rev(order(as.Date(tweets$Date, format="%Y-%m-%d"))),]

newsReuters <- read.csv("numberOfPressReleasesReuters.csv", header = TRUE, sep = ",") #loading dataset 'number of press releases (reuters.com) per day'
# adds dates without press releases to dataset
newsReutersWithAllDates <- sqldf("select bcData.Date, newsReuters.nNewsReuters from bcData left join newsReuters on bcData.Date=newsReuters.Date")
newsReutersWithAllDates[is.na(newsReutersWithAllDates)] <-0 #puts 0 to dates without press releases
rm(newsReuters)

newsCryptocoin <- read.csv("numberOfPressReleasesCryptocoinNews.csv", header = TRUE, sep = ",") #loading dataset 'number of press releases (cryptocoinnews.com) per day'
# adds dates without press releases to dataset
newsCryptocoinWithAllDates <- sqldf("select bcData.Date, newsCryptocoin.nNewsCryptocoin from bcData left join newsCryptocoin on bcData.Date=newsCryptocoin.Date")
newsCryptocoinWithAllDates[is.na(newsCryptocoinWithAllDates)] <-0 #puts 0 to dates without press releases
rm(newsCryptocoin)

googleTrends <- read.csv("googleTrends.csv", header = TRUE, sep = ",") #loading dataset 'worldwide search interest for "bitcoin" on google per week'
googleTrends$weekNumber <- week(googleTrends$week)
googleTrends$year <- year(googleTrends$week)

topUser <- read.csv("AllThingsAltBitcoins.csv", header = TRUE, sep = ",")
topUser <- topUser[rev(order(as.Date(topUser$Date, format="%Y-%m-%d"))),]

# inclusion of columns to bcData
# ------------------------------

bcData <- bcData[rev(order(as.Date(bcData$Date, format="%Y-%m-%d"))),]


bcData$year <- year(bcData$Date)
bcData$month <- month(bcData$Date)
bcData$week <- week(bcData$Date)

bcData <- sqldf("select bcData.Date, bcData.year, bcData.month, bcData.week, baverageUsd.X24hAverage, baverageUsd.Ask, baverageUsd.Bid, baverageUsd.Last, baverageUsd.totalVolume, bcData.numberOfTransactions  from bcData left join baverageUsd on bcData.Date=baverageUsd.Date")
# adds new column "numberOfTransactions" to bcData, takes Date from transactions (largest and most complete dataset)
rm(baverageUsd)

bcData$X24hAverage <- as.double(bcData$X24hAverage)

bcData["nTweets"] <- NA               # adds new column "nTweets" to bcData
bcData$nTweets[1:37] <- tweets$NumberOfTweets
rm(tweets)

bcData <- sqldf("select bcData.*, newsReutersWithAllDates.nNewsReuters from bcData left join newsReutersWithAllDates on bcData.Date=newsReutersWithAllDates.Date")
bcData$nNewsReuters[901:2763] <- NA #puts NA to dates before beginning of collecting news
rm(newsReutersWithAllDates)

bcData <- sqldf("select bcData.*, newsCryptocoinWithAllDates.nNewsCryptocoin from bcData left join newsCryptocoinWithAllDates on bcData.Date=newsCryptocoinWithAllDates.Date")
bcData$nNewsCryptocoin[914:2763] <- NA #puts NA to dates before beginning of collecting news
rm(newsCryptocoinWithAllDates)

# adding column "searchInterest"

bcData <- sqldf("select bcData.*, googleTrends.searchInterest from bcData left join googleTrends on (bcData.week=googleTrends.weekNumber and bcData.year=googleTrends.year)")
rm(googleTrends)

bcData["nTweetsTopUser"] <- NA               # adds new column "nTweets" to bcData
bcData$nTweetsTopUser[1:35] <- topUser$nTweets
rm(topUser)

# exporting main data set
# -----------------------
#write.csv(bcData, "bcData.csv", row.names=FALSE, quote=FALSE)   #creates new csv with consolidated data

head(bcData) # first rows of dataset

summary(bcData) # detailed overview

attach(bcData)

options(scipen=999)

rdate <- as.Date(bcData$Date, "%Y-%m-%d") # R-understandable date format

# 24h average value over time
plot(x=rdate[1:2301], y=bcData$X24hAverage[1:2301], xlab = "time", ylab = "daily average course in USD", type="l", col="dark red")

# number of transactions over time
plot(x=rdate, y=bcData$numberOfTransactions, xlab = "time", ylab = "number of transactions",  type="l")


# number of tweets over time
plot(x=rdate[1:37], y=bcData$nTweets[1:37], xlab = "time", ylab = "number of \"#bitcoin\"-tweets")

# number of news (reuters) over time

plot(x=rdate[1:901], y=bcData$nNewsReuters[1:901], xlab = "time", ylab = "number of news about \"bitcoin\" on reuters.com")

# number of news (cryptocoin) over time

plot(x=rdate[1:913], y=bcData$nNewsCryptocoin[1:913], xlab = "time", ylab = "number of news cryptocoinnews.com")


# search interest over time

plot(x=rdate[1:2035], y=bcData$searchInterest[1:2035], xlab = "time", ylab = "relative search interest over time", type = "l")



# regression tests
# ----------------

# nTweets ~ X24hAverage

plot(x=nTweets, y=X24hAverage, xlab = "number of \"#bitcoin\"-tweets per day", ylab = "daily average course in USD")

fitTweetsAvg <- lm(X24hAverage~nTweets)

abline(fitTweetsAvg, col="red")

summary(fitTweetsAvg) # Slope of regression line is not significantly different from 0 --> no real trend visible from data

plot(resid(fitTweetsAvg)) # plotting the residuals (= differences between observed value and estimated value)

shapiro.test(resid(fitTweetsAvg)) # residuals should be normally distributed --> p-value of Shapiro-Wilk normality-test < 0,05, thus they are not normally distributed

acf(resid(fitTweetsAvg))

# nTweets ~ numberOfTransactions

plot(x=nTweets, y=numberOfTransactions, xlab = "number of \"#bitcoin\"-tweets per day", ylab = "number of transactions")

fitTweetsnumberOfTransactions <- lm(numberOfTransactions~nTweets)

abline(fitTweetsnumberOfTransactions, col = "red")

summary(fitTweetsnumberOfTransactions)


# numberOfPressReleasesReuters ~ X24hAverage

plot(x=nNewsReuters, y=X24hAverage, xlab = "daily number of news on reuters.com", ylab = "daily average course in USD")

fitNewsAvg <- lm(X24hAverage~nNewsReuters)

abline(fitNewsAvg, col = "red")

summary(fitNewsAvg)

plot(resid(fitNewsAvg)) # strong pattern visible for residuals

acf(resid(fitNewsAvg)) # residuals strongly autocorrelated, model not usable

# numberOfPressReleasesReuters ~ numberOfTransactions

plot(x=nNewsReuters, y=numberOfTransactions)

fitNewsnTrans <- lm(numberOfTransactions~nNewsReuters)

abline(fitNewsnTrans)

summary(fitNewsnTrans) # no significance 

# numberOfPressReleasesCryptocoinnews ~ X24hAverage

plot(x=nNewsCryptocoin, y=X24hAverage)

fitNewsAvg <- lm(X24hAverage~nNewsCryptocoin)

abline(fitNewsAvg)

summary(fitNewsAvg)


# numberOfPressReleasesCryptocoinnews ~ numberOfTransactions

plot(x=nNewsCryptocoin, y=numberOfTransactions, xlab = "number of news on cryptocoinnews.com", ylab = "number of transactions")

fitNewsnTrans <- lm(numberOfTransactions~nNewsCryptocoin)

abline(fitNewsnTrans, col = "red")

summary(fitNewsnTrans)

plot(resid(fitNewsnTrans))  # strong trend visible for residuals

acf(resid(fitNewsnTrans)) # strong autocorrelation of residuals, model not usable

# nTweetsTopUser ~ X24hAverage

plot(x=nTweetsTopUser, y=X24hAverage, xlab = "number of \"#bitcoin\"-tweets per day \n by \"AllThingsAltBitcoins\" (490.000 tweets, 4520 follower)", ylab = "daily average course in USD")

fitTweetsAvgTopUser <- lm(X24hAverage~nTweetsTopUser)

abline(fitTweetsAvgTopUser, col="red")

summary(fitTweetsAvgTopUser)

# nTweetsTopUser ~ numberOfTransactions

plot(x=nTweetsTopUser, y=numberOfTransactions, xlab = "number of \"#bitcoin\"-tweets per day \n by \"AllThingsAltBitcoins\" (490.000 tweets, 4520 follower)", ylab = "number of transactions")

fitTweetsnumberOfTransactionsTopUser <- lm(numberOfTransactions~nTweetsTopUser)

abline(fitTweetsnumberOfTransactionsTopUser, col = "red")

summary(fitTweetsnumberOfTransactionsTopUser)



# grouping dataset by year and week

weeklyData <- sqldf("select bcData.Date, bcData.year, bcData.week, avg(bcData.X24hAverage), avg(bcData.numberOfTransactions), avg(bcData.nTweets), avg(bcData.nNewsReuters), avg(bcData.nNewsCryptocoin), avg(bcData.searchInterest) from bcData group by bcData.year, bcData.week")
weeklyData$`avg(bcData.X24hAverage)` <- as.integer(weeklyData$`avg(bcData.X24hAverage)`)
weeklyData$`avg(bcData.numberOfTransactions)` <- as.integer(weeklyData$`avg(bcData.numberOfTransactions)`)
weeklyData$`avg(bcData.nNewsReuters)` <- as.integer(weeklyData$`avg(bcData.nNewsReuters)`)
weeklyData$`avg(bcData.nNewsCryptocoin)` <- as.integer(weeklyData$`avg(bcData.nNewsCryptocoin)`)
weeklyData$`avg(bcData.searchInterest)` <- as.integer(weeklyData$`avg(bcData.searchInterest)`)

# searchInterest ~ X24hAverage

regrSearchInterestAvg <- lm(weeklyData$`avg(bcData.searchInterest)`[1:2035]~weeklyData$`avg(bcData.X24hAverage)`[1:2035])

plot(x=weeklyData$`avg(bcData.searchInterest)`[1:2035], y=weeklyData$`avg(bcData.X24hAverage)`[1:2035], xlab = "relative search interest for \"bitcoin\"", ylab = "daily average course in USD")

abline(regrSearchInterestAvg, col = "red")

summary(regrSearchInterestAvg)

# X24hAverage ~ searchInterest (from 2013)

regrSearchInterestAvg <- lm(weeklyData$`avg(bcData.X24hAverage)`[1:1304]~weeklyData$`avg(bcData.searchInterest)`[1:1304])

plot(x=weeklyData$`avg(bcData.searchInterest)`[1:1304], y=weeklyData$`avg(bcData.X24hAverage)`[1:1304], xlab = "relative search interest for \"bitcoin\"", ylab = "daily average course in USD")

abline(regrSearchInterestAvg, col = "red")

summary(regrSearchInterestAvg)

# searchInterest ~ numberOfTransactions

regrSearchInterestTrans <- lm(weeklyData$`avg(bcData.numberOfTransactions)`[1:2035]~weeklyData$`avg(bcData.searchInterest)`[1:2035])

plot(x=weeklyData$`avg(bcData.searchInterest)`[1:2035], y=weeklyData$`avg(bcData.numberOfTransactions)`[1:2035], xlab = "relative search interest for \"bitcoin\"", ylab = "number of transactions")

abline(regrSearchInterestTrans, col = "red")

summary(regrSearchInterestTrans)

detach(bcData)