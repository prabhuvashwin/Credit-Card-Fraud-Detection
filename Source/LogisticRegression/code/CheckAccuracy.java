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



public class CheckAccuracy extends Configured implements Tool {

	//FOR Gradient Descent
	private static final Logger LOG = Logger.getLogger(CheckAccuracy.class);
	private final static IntWritable one  = new IntWritable( 1);

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new CheckAccuracy(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), " accuracy ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));  

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			String[] array = line.split("#####");
			
			double predicted = Double.parseDouble(array[1]);
			double actual = Double.parseDouble(array[2]);
			
			if(predicted >= 0.5 && actual == 1){
				//True positive
				context.write(new Text("TruePositive"), one);
			}
			else if(predicted < 0.5 && actual == 0){
				//True negative
				context.write(new Text("TrueNegative"), one);
			}
			else if(predicted >= 0.5 && actual == 0){
				//False postive
				context.write(new Text("FalsePositive"), one);
			}
			else if(predicted < 0.5 && actual == 1){
				//False negative
				context.write(new Text("FalseNegative"), one);
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
