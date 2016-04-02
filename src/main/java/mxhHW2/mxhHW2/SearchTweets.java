package mxhHW2.mxhHW2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class SearchTweets extends Q4
{
	public List<Status> Search(String topic,String timeline) throws IOException{
		//org.apache.log4j.BasicConfigurator.configure();
		//OAuth Configuration
		ConfigurationBuilder cb = new ConfigurationBuilder();
    	cb.setDebugEnabled(true)
    	  .setOAuthConsumerKey("ArXdgVEiWGTTUH8fPz1zypTTY")
    	  .setOAuthConsumerSecret("NHP1yRDoD3x6HznLAKLevE70wEuSw7FVQL6Lg2PSI3LvgeFTK5")
    	  .setOAuthAccessToken("4836303139-iZuj2DRjnN1QeT3oRrNTG9FH9sVlSsF68V7LtyU")
    	  .setOAuthAccessTokenSecret("S9h3I2dM9j4pbhex7F2lkwpD6js70YjEsxbA9DKl5oOOL");
    	
    	TwitterFactory tf = new TwitterFactory(cb.build());
    	Twitter twitter = tf.getInstance();
    	Query query = new Query(topic);
    	//set timeline
    	query.setSince(timeline);
    	query.setLang("en");
    	//set maximum numbers of tweets
    	int MaxTweets = 1500;
    	int remainingTweets = MaxTweets;
    	//get ID of last tweets we fetched from Twitter
    	long lastID = Long.MAX_VALUE;
    	//Arraylist for storing tweets
    	List<Status> tweets = new ArrayList<Status>();
    	//fetch tweets
    	try
    	{ 
    	 while(remainingTweets > 0)
    	  {
    	    remainingTweets = MaxTweets - tweets.size();
    	    if(remainingTweets > 100) query.count(100);
    	    else query.count(remainingTweets); 
    	    QueryResult result = twitter.search(query);
    	    tweets.addAll(result.getTweets());
    	    Status s = tweets.get(tweets.size()-1);
    	    lastID = s.getId();
    	    query.setMaxId(lastID);
    	    remainingTweets = MaxTweets - tweets.size();
    	  }
    	 System.out.println("gathered "+tweets.size()+" tweets" );
    	}
    	catch(TwitterException te)
    	{
    	  System.out.println("Failed to search tweets: " + te.getMessage());
    	  System.exit(-1);
    	}
    	
    return tweets;
}
	public List<Status> Search1(String topic,String timeline) throws IOException{
		org.apache.log4j.BasicConfigurator.configure();
		//OAuth Configuration
		ConfigurationBuilder cb = new ConfigurationBuilder();
    	cb.setDebugEnabled(true)
    	  .setOAuthConsumerKey("VqOrvXICD2HXETkeM9gDnEOFB")
    	  .setOAuthConsumerSecret("p25tQ8k3A12JBUyWiKppk8KJl65Fd3vED3eIlobbIJKSONL6a7")
    	  .setOAuthAccessToken("4836303139-uBPK54GipQWCy8X4ruIdOC5jbPfpu1bU4DGOoSE")
    	  .setOAuthAccessTokenSecret("TRFKoEQhOX2dO7enOUeoXHbw9PPTsIioR0GDFqksa9bFO");
    	
    	TwitterFactory tf = new TwitterFactory(cb.build());
    	Twitter twitter = tf.getInstance();
    	Query query = new Query(topic);
    	//set timeline
    	query.setSince(timeline);
    	query.setLang("en");
    	//set maximum numbers of tweets
    	int MaxTweets = 1500;
    	int remainingTweets = MaxTweets;
    	//get ID of last tweets we fetched from Twitter
    	long lastID = Long.MAX_VALUE;
    	//Arraylist for storing tweets
    	List<Status> tweets = new ArrayList<Status>();
    	//fetch tweets
    	try
    	{ 
    	 while(remainingTweets > 0)
    	  {
    	    remainingTweets = MaxTweets - tweets.size();
    	    if(remainingTweets > 100) query.count(100);
    	    else query.count(remainingTweets); 
    	    QueryResult result = twitter.search(query);
    	    tweets.addAll(result.getTweets());
    	    Status s = tweets.get(tweets.size()-1);
    	    lastID = s.getId();
    	    query.setMaxId(lastID);
    	    remainingTweets = MaxTweets - tweets.size();
    	  }
    	 System.out.println("gathered "+tweets.size()+" tweets" );
    	}
    	catch(TwitterException te)
    	{
    	  System.out.println("Failed to search tweets: " + te.getMessage());
    	  System.exit(-1);
    	}
    	
    return tweets;
}
	public void FileCopyToHDFS(List<Status> tweets,String dst) throws IOException{
	    //convert tweets array to a stringbuilder
		StringBuilder sb = new StringBuilder();
	    for(Status s : tweets){
	        sb.append("@"+s.getUser().getScreenName()+":"+s.getText()+"\r\n");           
	    }
	    //convert stringbuilder to inputstream
	    ByteArrayInputStream stream = new ByteArrayInputStream( sb.toString().getBytes("UTF-8") );
	    
	    //hadoop configuration
	    Configuration conf = new Configuration();
	    //overwrite the configuration file
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
	    
	    	FileSystem fs = FileSystem.get(URI.create(dst), conf); //create filesystem for the new file
		    OutputStream out = fs.create(new Path(dst), new Progressable() {
		      public void progress() {
		        System.out.print(".");
		      }
		    });
		    IOUtils.copyBytes(stream, out, 4096, true);
			System.out.println();
		    System.out.println("-------------------------downloaded tweets to the HDFS----------------------------");

	  }
    public static void main( String[] args ) throws TwitterException, Exception
    {
		SearchTweets st = new SearchTweets();
	    String topic = "star wars";
	    System.out.println("Starting tweets search, the topic is:"+topic);
	    String dst = args[0];
	    	//download tweets
		List<Status> ls = st.Search(topic,"2015-01-08");
		st.FileCopyToHDFS(ls, dst+"/1.txt");
		List<Status> ls1 = st.Search(topic,"2015-03-08");
	    st.FileCopyToHDFS(ls1, dst+"/2.txt");
	    List<Status> ls2 = st.Search(topic,"2015-05-08");
	    st.FileCopyToHDFS(ls2, dst+"/3.txt");
	    List<Status> ls3 = st.Search1(topic,"2015-07-08");
	    st.FileCopyToHDFS(ls3, dst+"/4.txt");
	    List<Status> ls4 = st.Search1(topic,"2015-09-08");
	    st.FileCopyToHDFS(ls4, dst+"/5.txt");
	    List<Status> ls5 = st.Search1(topic,"2015-11-08");
	    st.FileCopyToHDFS(ls5, dst+"/6.txt");
	    System.out.println("-----------------------starting MapReduce-----------------------");	
	    //start map reduce
	    Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	    conf.set("mapreduce.framework.name", "yarn");
	    Job job = Job.getInstance(conf, "Top N");
	    job.setJar("1.jar");
	    job.setMapperClass(Q4.TopNMapper.class);
	   // job.setCombinerClass(Q4.TopNCombiner.class);
	    job.setReducerClass(Q4.TopNReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}
