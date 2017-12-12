package xyz.jason5544.test;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class wcMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	protected void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException
	{
		String line = value.toString(); 
		StringTokenizer st = new StringTokenizer(line);
		while (st.hasMoreTokens())
		{
			String world = st.nextToken();
			context.write(new Text(world), new IntWritable(1));
		}
	}
}

