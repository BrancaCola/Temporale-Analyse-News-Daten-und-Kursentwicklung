package big.data;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;
import java.util.TimeZone;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.Substring;

import scala.Tuple2;




public class App 
{

	public static void main( String[] args ) throws java.text.ParseException, IOException
	{
		//count news per day
//		countDatesReuters();
//		countDatesCryptocoins();
//		googleTrends();
		
		//read specific tweet file and count tweets per day
//		countTweetsPerDay();
		
		//read jsons and save as one resource
//		unionTweets();
		
		//read all tweets with sql and count them
//		tweetsHandler();
    }

  
    //..a method which search the date in each line and count each day inkrementally
	public static void countDatesReuters(){
		String inputFileName = "samples/reuters_search_bitcoin.txt" ;
	    String outputDirName = "output" ;
	    SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
	    JavaSparkContext context = new JavaSparkContext(conf);
	    JavaRDD<String> file = context.textFile(inputFileName);
	    //separates each line
		JavaRDD<String> lines = file.flatMap(new FlatMapFunction<String, String>() {public Iterable<String> call(String s) { return Arrays.asList(s.split(System.getProperty("line.separator")));}});
		//search specific format
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) throws java.text.ParseException { 
				try{
					SimpleDateFormat sd = new SimpleDateFormat("MMMM dd, yyyy", Locale.ENGLISH);
					SimpleDateFormat sd2 = new SimpleDateFormat("yyyy-MM-dd");
					Date specificDay = sd.parse(s);
					String newDate = sd2.format(specificDay);
					return new Tuple2<String, Integer>(newDate, 1);
				}catch(Exception e){
					return new Tuple2<String, Integer>("", 1);
				}
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer a, Integer b) { return a + b; }
		});
	
		counts.saveAsTextFile(outputDirName);
				
	}


	//..a method which search the date in each line and count each day inkrementally
	public static void countDatesCryptocoins(){
		String inputFileName = "samples/cryptocoinsnews-pressreleases.txt" ;
	    String outputDirName = "output" ;
	    SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
	    JavaSparkContext context = new JavaSparkContext(conf);
	    JavaRDD<String> file = context.textFile(inputFileName);
	    //separates each line
		JavaRDD<String> lines = file.flatMap(new FlatMapFunction<String, String>() {public Iterable<String> call(String s) { return Arrays.asList(s.split(System.getProperty("line.separator")));}});
		//search specific format
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) { 
				if(s.matches("\\d{2}/\\d{2}/\\d{4}")){
					String newFormat = ""+s.charAt(6)+s.charAt(7)+s.charAt(8)+s.charAt(9)+"-"+s.charAt(3)+s.charAt(4)+"-"+s.charAt(0)+s.charAt(1);
					return new Tuple2<String, Integer>(newFormat, 1);
				}else return new Tuple2<String, Integer>("", 1);
			}
		});
	
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer a, Integer b) { return a + b; }
		});
	
		counts.saveAsTextFile(outputDirName);
				
	}


	public static void googleTrends() throws FileNotFoundException, java.text.ParseException{
		
		//2004-01-04 - 2004-01-10,0
		//lines like this show the trend at google --> for one week, starting with the first date and a relativ trend [%] after the ","
		
		String inputFileName = "samples/bitcoin_google_trends_report.txt";
	    String outputDirName = "output" ;
	    File f = new File(inputFileName);
		Scanner s = new Scanner(f);
		ArrayList<String> dates = new ArrayList<String>();
		SimpleDateFormat firstWeekdayFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		while(s.hasNextLine()){
			String amount, date, line = s.nextLine();
			if(line.length()==0)break;
			date = line.substring(0, 10);
			amount = line.substring(24,line.length());
			Date sDate = new Date();
			Date anotherDay = new Date();
			sDate = firstWeekdayFormat.parse(date);
			
			dates.add(date+", "+amount);
			//generating the following dates
			Calendar c = Calendar.getInstance(); 
			c.setTime(sDate); 
			c.add(Calendar.DATE, 1);
			anotherDay = c.getTime();
			dates.add(firstWeekdayFormat.format(anotherDay)+", "+amount);
			c.setTime(sDate); 
			c.add(Calendar.DATE, 2);
			anotherDay = c.getTime();
			dates.add(firstWeekdayFormat.format(anotherDay)+", "+amount);
			c.setTime(sDate); 
			c.add(Calendar.DATE, 3);
			anotherDay = c.getTime();
			dates.add(firstWeekdayFormat.format(anotherDay)+", "+amount);
			c.setTime(sDate); 
			c.add(Calendar.DATE, 4);
			anotherDay = c.getTime();
			dates.add(firstWeekdayFormat.format(anotherDay)+", "+amount);
			c.setTime(sDate); 
			c.add(Calendar.DATE, 5);
			anotherDay = c.getTime();
			dates.add(firstWeekdayFormat.format(anotherDay)+", "+amount);
			c.setTime(sDate); 
			c.add(Calendar.DATE, 6);
			anotherDay = c.getTime();
			dates.add(firstWeekdayFormat.format(anotherDay)+", "+amount);    		
		}
		File output = new File("samples/googleTrend.txt");
		
		try{
			listInDatei(dates, output);
		}catch(Exception e){
			
		}
		
	}


	public static void countTweetsPerDay() throws IOException{
    	boolean woPostprocessing = true;
    	SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("WARN");
		SQLContext sqlC = new SQLContext(context);
		DataFrameReader dfReader = new DataFrameReader(sqlC);		
		String query, path = "samples/allTweets";
		DataFrame input0 = dfReader.json(path); //dfReader.json("allTweets"); //json("allTweets"); //sqlC.read().json("allTweets");
		input0.registerTempTable("tweets");
		//tweets (id, message, date, user_id, user, followers)
		String user1 = "Redd Bazaar";
		query = "SELECT user, date, count(*) AS anzahl FROM tweets WHERE user = '"+user1+"' GROUP BY user, date ORDER BY date DESC"; woPostprocessing = false;
		//query = "SELECT user, count(*) AS anzahl FROM tweets GROUP BY user ORDER BY anzahl DESC LIMIT 5";
		DataFrame tweets = sqlC.sql(query);
        Row[] result = tweets.collect();
        if(result.length==0)System.out.println("Die Suche ergab keinen Treffer!");
        if(woPostprocessing){
        	for (Row row : result) {
                System.out.println("User: "+row.get(0)+" Tweets: "+ row.get(1));
              	//System.out.println("Datum: "+row.get(1)+" Tweets: "+ row.get(2)+" User "+row.get(0));
              }
        }else{
        	JavaPairRDD<String, Integer> pairs = tweets.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
    			public Tuple2<String, Integer> call(Row row) throws java.text.ParseException { 
    				try{
    					SimpleDateFormat sd = new SimpleDateFormat("MMM dd yyyy", Locale.ENGLISH);
    					SimpleDateFormat sd2 = new SimpleDateFormat("yyyy-MM-dd");
    					String s1, s2, s;
    					s1 = row.get(1).toString().substring(4,10);
    					s2 = row.get(1).toString().substring(26,30);
//    					return new Tuple2<String, Integer>("", 1);
    					s = s1 +" "+s2;
    					Date specificDay = sd.parse(s);
    					String newDate = sd2.format(specificDay);
    					int anz = Long.valueOf(row.getLong(2)).intValue();
    					return new Tuple2<String, Integer>(newDate, anz);
    				}catch(Exception e){
    					//System.out.println("keine Weitergabe!");
    					return new Tuple2<String, Integer>("", 1);
    				}
    			}
    		});
        	
        	
        	
    		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
    		  public Integer call(Integer a, Integer b) { return a + b; }
    		});
    		List<Tuple2<String, Integer>> list = counts.collect();
    		File file = new File ("out/"+user1+".txt");
    		file.getParentFile().mkdirs(); 
    		file.createNewFile();
    		for (Tuple2<String, Integer> tuple2 : list) {
				FileWriter fw = new FileWriter(file);
				
				fw.write(tuple2._1+", "+tuple2._2);
				System.out.println(tuple2);
				
			}
    		//counts.saveAsTextFile("out");
        }
    }
    
   
	public static void unionTweets(){
		String f1, f2, f3, f4, f5, f6, f7, f8, f9, f10;
		String[] files = new String [20];
		files[0]= "samples/tweets621.json";
		files[1]= "samples/tweets623.json";
		files[2]= "samples/tweets624.json";
		files[3]= "samples/tweets626.json";
		files[4]= "samples/tweets627.json";
		files[5]= "samples/tweets628.json";
		files[6]= "samples/tweets629.json";
		files[7]= "samples/tweets701.json";
		files[8]= "samples/tweets704.json";
		files[9]= "samples/tweets706.json";
		files[10]= "samples/tweets708.json";
		files[11]= "samples/tweets711.json";
		files[12]= "samples/tweets713.json";
		files[13]= "samples/tweets715.json";
		files[14]= "samples/tweets717.json";
		files[15]= "samples/tweets718.json";
		files[16]= "samples/tweets719.json";
		files[17]= "samples/tweets721.json";
		files[18]= "samples/tweets726.json";
		files[19]= "samples/tweets727.json";
		SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
	    JavaSparkContext context = new JavaSparkContext(conf);
		SQLContext sqlC = new SQLContext(context);
		
		DataFrame all, input0, in0, in1;
		input0 = sqlC.read().json(files[0]);
	    
	    // Print the schema
	    //input1.printSchema();
	    
		
		//Registers this DataFrame as a temporary table using tweets as name.
	    input0.registerTempTable("tweets");
	    /*
	     * Select tweets based on the amount of Retweets
	    DataFrame topTweets = sqlC.sql("SELECT text, user.name, retweet_count FROM tweets ORDER BY retweet_count DESC LIMIT 10");
	    Row[] result = topTweets.collect();
	    for (Row row : result) {
	      System.out.println(row.get(1)+" schrieb: "+row.get(0)+" Retweets: "+row.get(2));
	    }
	    ..there are always 0 Retweets in the Tweets --> cannot be used here. The reason is that the tweets are streamed directly
	    */
	    
	    in0 = sqlC.sql("SELECT id, text as message, created_at as date, user.id as user_id, user.name as user, user.followers_count as followers FROM tweets");
	    
	    all = in0;
	    
	    for(int file = 1; file < files.length;file++){
	    	input0 = sqlC.read().json(files[file]);
	    	input0.registerTempTable("tweets");
	    	in1 = sqlC.sql("SELECT id, text as message, created_at as date, user.id as user_id, user.name as user, user.followers_count as followers FROM tweets");
	        all = all.unionAll(in1);
	    }
	    
	    DataFrameWriter dfWriter = all.write();
	    dfWriter.json("samples/allTweets");
	    System.exit(0);
	    
	    
	    
	    
	    /*
	     * Select Tweets count per user
	     
	    DataFrame topTweets = sqlC.sql("SELECT user.name, count(*) AS anzahl FROM tweets GROUP BY user.name ORDER BY anzahl DESC LIMIT 10");
	    Row[] result = topTweets.collect();
	    for (Row row : result) {
	      System.out.println(row.get(1)+" Tweets geschrieben von: "+row.get(0));
	    }
	    System.exit(0);
	    
	    
	    JavaRDD<String> topTweetText = topTweets.toJavaRDD().map(new Function<Row, String>() {
	        public String call(Row row) {
	          return row.getString(0);
	        }});
	    System.out.println(topTweetText.collect());
	    */
	
	}


	public static void tweetsHandler(){
    	String FileName= "";
    	String outputDirName = "output" ;
    	
    	//define configuration
        SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        
        JavaRDD<String> file = context.textFile(FileName);
        
        //mapper
		JavaRDD<String> lines = file.flatMap(new FlatMapFunction<String, String>() {
		  public Iterable<String> call(String s) { return Arrays.asList(s.split(System.getProperty("line.separator")));}//"")); }
		
		});
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) { 
				String in = "", out = "";
				SimpleDateFormat sd = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
				SimpleDateFormat sd3 = new SimpleDateFormat("MMMddyyyy");
				SimpleDateFormat sd2 = new SimpleDateFormat("yyyy-MM-dd");
			    try{
	        		JSONObject obj= (JSONObject) new JSONParser().parse(s);
	                in=(String) obj.get("created_at");
	                in = in.substring(0, 20) + "EDT " + in.substring(26, 30);
			        Date datum = sd.parse(in);
			        out = sd2.format(datum);
			    }catch(Exception e){
			        		
			    }
			    return new Tuple2<String, Integer>(out, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer a, Integer b) { return a + b; }
		});
		counts.saveAsTextFile(outputDirName);
    	
        
   
    }

    //for saving the ArrayList into a file
    private static void listInDatei(List list, File datei) throws IOException {
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(new FileWriter(datei));
            Iterator iter = list.iterator();
            while(iter.hasNext() ) {
                Object o = iter.next();
                printWriter.println(o);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(printWriter != null) printWriter.close();
        }
    }
    
}
