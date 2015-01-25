/**
 * MapReduce program to find the number of male users who has rated a given movie
 * This program demonstrates the use of Distributed cache and Map-side join
 * 
 * @author Rahul
 *
 */
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class MapSideJoin {

	//Map class for performing map-side join
	public static class MapJoinMap extends Mapper<LongWritable, Text, IntWritable, Text> 
	{
		HashMap<String,String> usersMap = new HashMap<String,String>();
		int userCount=0;
		String movieID = null;
		Text one  = new Text();
		//Setup the users data from cache before the map function is executed
		public void setup(Context context) throws IOException
		{
			Configuration conf = context.getConfiguration();
			//Fetch the users data from the cache
			Path[] userFile = DistributedCache.getLocalCacheFiles(conf);
			//Fetch the movieId which is passed
			movieID = conf.get("movieID");
			for(Path givenPath:userFile)
			{
				try{
					//Identify file system(hdfs in this case) for the path of cache file
					FileSystem fs = givenPath.getFileSystem(conf);
					FSDataInputStream iStream = fs.open(givenPath);
					String line="";
					while((line=iStream.readLine())!= null)
					{
						String[] tokens = line.split("::");
						usersMap.put(tokens[0], tokens[1]);
					}
					iStream.close();
				}
				catch(Exception e)
				{
					System.out.println("Exception in setup: "+e.getMessage());
				}
			}
		}

		//Map function for performing the map-side join on user data and ratings data
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{   
			String[] inputFile = value.toString().split("::");
			String userId = inputFile[0];
			String movieId = inputFile[1];
			if(movieId.equals(movieID))
			{
				if (usersMap.containsKey(userId))
				{
					if((usersMap.get(userId)).equals("M"))
					{
						userCount++;
					}
				}
			} 
		} 

		//Output the final count of users who are male and have rated the given movie 
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(new IntWritable(userCount) , one);
		}
	}

	//Reducer class to output the final count of users
	public static class MapJoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
	{
		int totalCount =0;
		public void reduce(Iterable<IntWritable> key, Text value, Context context) throws IOException, InterruptedException
		{ 
			for(IntWritable k : key)
			{
				totalCount = k.get();
			}
			context.write(new IntWritable(totalCount), value);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		conf.set("movieID", args[3]);
		//setup Distributed cache for users data file (Passed as first argument to the program)
		DistributedCache.addCacheFile(new URI(args[0]),conf);
		Job CountJob = new Job(conf , "MapSideJoin");
		CountJob.setJarByClass(MapSideJoin.class);
		CountJob.setMapperClass(MapJoinMap.class);
		CountJob.setReducerClass(MapJoinReducer.class);
		CountJob.setOutputKeyClass(IntWritable.class);
		CountJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(CountJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(CountJob, new Path(args[2]));
		CountJob.waitForCompletion(true);
	}

}