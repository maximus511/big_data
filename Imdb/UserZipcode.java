/**
 * MapReduce program to retrieve all users having given zipcode.
 * 
 * @author Rahul
 *
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UserZipcode 
{
	//Map and reduce to filter users with given zipcode
	public static class Map extends	Mapper<LongWritable, Text, Text, IntWritable> 
	{
		String zipCode = "";
		private static final IntWritable one = new IntWritable(1);

		protected void setup(Context context) 
		{
			Configuration config = context.getConfiguration();
			zipCode = config.get("inputParameter");
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] currentUserTuple = line.split("::");
			if (currentUserTuple[4].trim().equalsIgnoreCase(zipCode))
			{
				context.write(new Text("User ID:: " + currentUserTuple[0].trim()), one);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			for (IntWritable value : values)
			{
				context.write(key,new IntWritable());
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		Configuration conf = new Configuration();
		conf.set("inputParameter", args[2]);
		Job job = new Job(conf, "UserZipcode");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(UserZipcode.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
