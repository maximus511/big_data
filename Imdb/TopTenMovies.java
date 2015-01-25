/**
 * MapReduce program to get top ten rated movies.
 * This program uses MapReduce chaining.
 * 
 * @author Rahul
 *
 */
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopTenMovies 
{
	//Map and Reduce classes for calculating average rating of each movie
	public static class AverageMap extends Mapper<LongWritable, Text, Text, DoubleWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] currentRatingsTuple = line.split("::");
			context.write(new Text(currentRatingsTuple[1]), new DoubleWritable(Double.valueOf(currentRatingsTuple[2])));
		}
	}
	public static class AverageReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
	{
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double sum=0, count= 0;
			for(DoubleWritable value : values)
			{
				sum+=value.get();
				count++;
			}
			double avg = (double)sum/count;
			context.write(key, new DoubleWritable(avg));
		}
	}	

	//Custom Comparator for comparison of double values
	public static class DoubleComparator extends WritableComparator 
	{
		public DoubleComparator() 
		{
			super(DoubleWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
			Double firstValue = ByteBuffer.wrap(b1, s1, l1).getDouble();
			Double secondValue = ByteBuffer.wrap(b2, s2, l2).getDouble();
			return firstValue.compareTo(secondValue) * (-1);
		}
	}

	//Map and Reduce to sort and get the top ten rated movies
	public static class TopTenMap extends Mapper<Object, Text, DoubleWritable, Text>
	{
		private HashMap<String, Double> topTen = new HashMap<String, Double>();
		public int counter =0;
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line = value.toString().split("\t");
			Double rating = Double.parseDouble(line[1]);

			Integer totalCount = 10;
			if(counter<totalCount)
			{
				topTen.put(line[0], rating);
				counter++;
			}
			else
			{
				String lowerKey=null;
				//Initializing with 10(any number greater than 5) as max value of rating is 5 (from ReadMe.txt)
				Double minimum = 10.0;
				for(String currentKey: topTen.keySet())
				{
					if(topTen.get(currentKey)< minimum)
					{
						minimum = topTen.get(currentKey);
						lowerKey = currentKey;
					}
				}
				if(rating >= minimum)
				{
					topTen.remove(lowerKey);
					topTen.put(line[0], rating);
				}
			}
		}
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException
		{
			for(String currentKey:topTen.keySet())
			{
				context.write( new DoubleWritable(topTen.get(currentKey)),new Text(currentKey));
			}
		}
	}
	public static class TopTenReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
	{
		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for (Text value : values)
			{
				context.write(value,key);
			}
		}
	}	

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		Configuration conf = new Configuration();
		Job calcAvgJob = new Job(conf, "Average");
		calcAvgJob.setOutputKeyClass(Text.class);
		calcAvgJob.setOutputValueClass(DoubleWritable.class);
		calcAvgJob.setJarByClass(TopTenMovies.class);
		calcAvgJob.setMapperClass(AverageMap.class);
		calcAvgJob.setReducerClass(AverageReduce.class);
		calcAvgJob.setInputFormatClass(TextInputFormat.class);
		calcAvgJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(calcAvgJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(calcAvgJob, new Path(args[1]));
		if(calcAvgJob.waitForCompletion(true))
		{
			Job calcTopTenJob = new Job(conf, "TopTen");
			calcTopTenJob.setOutputKeyClass(DoubleWritable.class);
			calcTopTenJob.setOutputValueClass(Text.class);
			calcTopTenJob.setJarByClass(TopTenMovies.class);
			calcTopTenJob.setMapperClass(TopTenMap.class);
			calcTopTenJob.setReducerClass(TopTenReduce.class);
			calcTopTenJob.setSortComparatorClass(DoubleComparator.class);
			calcTopTenJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(calcTopTenJob, new Path(args[1]));
			FileOutputFormat.setOutputPath(calcTopTenJob, new Path(args[2]));
			calcTopTenJob.waitForCompletion(true);
		}																																																																																																																																																																													
	}
}






