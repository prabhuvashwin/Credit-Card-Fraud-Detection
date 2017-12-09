package com.cloud.lrgd;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class CountLines extends Configured implements Tool {
	
	//FOR Gradient Descent
	private static final Logger LOG = Logger.getLogger(CountLines.class);
	private final static IntWritable one  = new IntWritable( 1);
	
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new CountLines(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), " countlines ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));  // "inputDirectory/count"

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			String line = lineText.toString();
			String[] array = line.split(",");
			if(line.contains("V1")){  //should be changed for other datasets
				//skip the attribute name row;
				LOG.info("Coloumns Count");
				//number of columns will be the size of array
				int num_cloumns = array.length;
				
				context.write(new Text("columns"), new IntWritable(num_cloumns));
				
			}
			else{
				context.write(new Text("rows"), one);
			}
		}		
	}
	
	public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			int sum  = 0;
			//counting rows and columns
			for ( IntWritable count : counts) {
				sum  += count.get();
			}
			context.write(word,  new IntWritable(sum));
		}
	}
	

}
