package xyz.jason5544.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wc
{
	public static void main(String[] args)
	{
		Configuration conf = new Configuration();
		try
		{
			Job job = new Job(conf);
			job.setJobName("wc");
			job.setJarByClass(wc.class);;
			job.setMapperClass(wcMap.class);
			job.setReducerClass(wcReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setNumReduceTasks(2);

			FileInputFormat.addInputPath(job, new Path("hdfs://49.123.68.223:9000/wc/input/"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://49.123.68.223:9000/wc/output/"));
			System.exit(job.waitForCompletion(true)?0:1);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}