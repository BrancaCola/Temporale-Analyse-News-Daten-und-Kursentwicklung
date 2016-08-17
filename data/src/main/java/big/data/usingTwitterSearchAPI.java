package big.data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDD.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;


public class usingTwitterSearchAPI {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String inputFileName = "samples/test.txt" ;
        String outputDirName = "output" ;
        int exceptionnumber=0;
        String key, secret, token, tokensecret;
        ConfigurationBuilder cf;
        Date minDate, maxDate;
        
        cf = new ConfigurationBuilder();
        
		//connection to the twitter APP
        key = args[0];
        secret = args[1];
        token = args[2];
        tokensecret = args[3];
        cf.setDebugEnabled(true)
        	.setOAuthConsumerKey(key)
        	.setOAuthConsumerSecret(secret)
        	.setOAuthAccessToken(token)
        	.setOAuthAccessTokenSecret(tokensecret)
        	;
        
        TwitterFactory tf = new TwitterFactory(cf.build());
        twitter4j.Twitter twitter = tf.getInstance();
        
        //create query with #Bitcoin as filter
        Query query = new Query("#Bitcoin");
	    //number of Tweets to query for
        int numberOfTweets = 1000000;
	    long lastID = Long.MAX_VALUE;
	    ArrayList<Status> tweets = new ArrayList<Status>();
	    while (tweets.size () < numberOfTweets) {
	      if (numberOfTweets - tweets.size() > 1000)
	        query.setCount(1000);
	      else 
	        query.setCount(numberOfTweets - tweets.size());
	      
	      try {
	        QueryResult result = twitter.search(query);
	        tweets.addAll(result.getTweets());
	        //printing the amount of tweets
	        System.out.println("Bereits " + tweets.size() + " Tweets gesammelt");
	        for (Status t: tweets) 
	          if(t.getId() < lastID) 
	              lastID = t.getId();
	
	      }
	
	      catch (TwitterException te) {
	        System.out.println("\n" + "Verbindung nicht möglich, Exception: " + te + "\n");
	        exceptionnumber++;
	        if(exceptionnumber >1)break;
	      }; 
	      query.setMaxId(lastID-1);
	    }
	    
	    int anz = 0;
	    //to get to know in which time period tweets are fetched we use minDate and maxDate
	    minDate = tweets.get(0).getCreatedAt();
	    maxDate = tweets.get(0).getCreatedAt();
	    
	    for (int i = 0; i < tweets.size(); i++) {
	      Status t = (Status) tweets.get(i);
	      anz++;
	      Date datum = t.getCreatedAt();
	      
	      /*
	      //more possible information
	      GeoLocation loc = t.getGeoLocation();
	      String user = t.getUser().getScreenName();
	      String msg = t.getText();
	      if (loc!=null) {
	    	  Double lat = t.getGeoLocation().getLatitude();
	    	  Double lon = t.getGeoLocation().getLongitude();
	      }
	      //some possible prints
	      System.out. print(i + " DATUM: "+datum+" USER: " + user + " wrote: " + msg + "\n");
	      List<Status> status = twitter.getHomeTimeline();
	      for (Status st : status){
	    	  System.out.println(st.getUser().getName()+"----"+st.getText());
	      }
	      */
	      
	      //compare the dates
	      if(minDate.after(datum))minDate=datum;
	      if(maxDate.before(datum))maxDate=datum;
	      
        }
	    System.out.println("Das erste Datum ist: "+minDate+ "\n"+"Das letzte Datum ist: "+ maxDate);
	}
}
