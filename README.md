# Temporale-Analyse-News-Daten-und-Kursentwicklung

Project using Spark, Twitter Streaming API and R to explore the relation between news and course data for Bitcoin

# Install
Linux 14.04 LTS
Eclipse Mars
Spark R
Dependencies (see pom.xml)

# Data Import
Fetching News by searching for bitcoin News
  googleTrendsReport (bitcoin_google_trends_report.txt)
  reuters (reuters_search_bitcoin.txt)
  cryptocoins (cryptocoinsnews-pressreleases.txt)
Fetching Tweets about Bitcoin with Twitter Streaming API
  there is a file which shows the fetched tweets (tweets701.json)
  there is an example for searching for tweets with Twitter Search API (usingTwitterSearchAPI.java)
  there are tweets saved as DataSet HDFS Format (/allTweets folder) which is done with unionTweets() method

# Preprocessing
reading files and save the amount of news per day (countDatesReuters(),countDatesCryptocoins(),googleTrends())
count tweets per specific day (countTweetsPerDay())
save all available tweets from json files as one resource (unionTweets())
query tweets with sql and search for relevant user and the amount of tweets per day (tweetsHandler())

