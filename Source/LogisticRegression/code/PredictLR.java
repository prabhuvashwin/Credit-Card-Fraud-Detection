package com.cloud.lrgd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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


/*
 * It will predict the outcome on test data set
 * 
 * */

public class PredictLR extends Configured implements Tool{

	private static final Logger LOG = Logger.getLogger(PredictLR.class);
	HashMap<String, String> hashMap = new HashMap<>();
	HashMap<String, String> rowColumnValues = new HashMap<>();
	HashMap<String, String> betaValues = new HashMap<>();


	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new PredictLR(), args);
		System.exit(res);
	}

	private HashMap<String, String> getParams(Path path) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fileSystem.open(path)));

		LOG.info("PATH "+path.toString() );

		String line;	
		String temp="";
		while((line = br.readLine())!=null){

			if(line.contains("columns")){
				temp = "columns";
			}
			if(line.contains("rows")){
				temp = "rows";
			}
			if(line.contains("beta")){
				temp = "beta";
			}

			line = line.replace(temp, "").trim();
			hashMap.put(temp, line);
			LOG.info("hash-------------temp> "+temp+" <>line>> "+line);
		}

		return hashMap;
	}


	public int run( String[] args) throws  Exception {

		Job job = null;
		Configuration conf = new Configuration();

		//getting rows and column number
		rowColumnValues = getParams(new Path(args[3]+"/part-r-00000"));

		//getting beta coefficients
		betaValues = getParams(new Path(args[2]+"/part-r-00000"));

		conf.set("beta", betaValues.get("beta"));
		conf.set("rows", rowColumnValues.get("rows"));
		conf.set("columns", rowColumnValues.get("columns"));

		LOG.info("test > "+betaValues.get("beta"));
		LOG.info("rows "+rowColumnValues.get("rows"));
		LOG.info("args[1]----test "+args[1]);

		job = Job.getInstance(conf, " predict");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			double prediction = 0.0;
			//long param = Long.parseLong(conf.get("DocCount"));
			Configuration conf = context.getConfiguration();
			String betaString = conf.get("beta").toString();
			LOG.info("BETASTRINGtest "+betaString);

			String line  = lineText.toString();
			String[] array = line.split(",");

			int len = array.length;
			LOG.info("array.length ....---| "+len);

			if(line.contains("V1")){
				//skip the 1st row
				LOG.info("skipped");
			}
			else{

				String[] coefArray = betaString.split("%%%%%");
				int l = coefArray.length;
				LOG.info("len of coefArray > "+l);
				LOG.info("len of lineArray > "+len);
				double sum= Double.parseDouble(coefArray[0]);
				for(int i=0;i<len-1;i++){

					double x1 = Double.parseDouble(array[i]);
					double b1 = Double.parseDouble(coefArray[i+1]);
					sum = sum + (b1*x1);


				}
				//calculating the prediction using logistic function
				LOG.info("map sum test else block.... "+sum);
				prediction = 1/(1+Math.exp(-sum));
				LOG.info("map prediction test else block.... "+prediction);

				context.write(new Text(lineText), new Text(String.valueOf(prediction)+"#####"+array[len-1]));
			}
		}
	}

	public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {
			
			String s="";
			for ( Text count  : counts) {
				s = s + count.toString();
			}
			
			context.write(new Text(word+"#####"),  new Text(s));
		}
	}

}
