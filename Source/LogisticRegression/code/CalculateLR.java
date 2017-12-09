package com.cloud.lrgd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
 * CalculateLR will calculate the new beta coefficients
 * 
 * */

public class CalculateLR extends Configured implements Tool{

	private static final Logger LOG = Logger.getLogger(CalculateLR.class);
	HashMap<String, String> hashMap = new HashMap<>();
	HashMap<String, String> rowColumnValues = new HashMap<>();
	HashMap<String, String> betaValues = new HashMap<>();

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new CalculateLR(), args);
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


	public int run(String[] args) throws Exception {
		Job job = null;

		Configuration conf = new Configuration();
		
			
		//getting rows and column number
		rowColumnValues = getParams(new Path(args[3]+"/part-r-00000"));
		
		//getting beta coefficients
		LOG.info("itr01 "+args[1]);
		if(args[1].equals("iteration1")){
			// no i/p location for beta, all coefficients are zero. Prediction will be 0.5
			conf.set("beta", "invalid");
			LOG.info("test1 "+args[1]);
			
		}
		else{
			betaValues = getParams(new Path(args[1]+"/part-r-00000"));
			LOG.info("test > "+betaValues.get("beta"));
			conf.set("beta", betaValues.get("beta"));
			
		}
		
		LOG.info("rows "+rowColumnValues.get("rows"));
		
		conf.set("rows", rowColumnValues.get("rows"));
		conf.set("columns", rowColumnValues.get("columns"));

		job = Job.getInstance(conf, " calculate");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			double prediction = 0.0;
			//long param = Long.parseLong(conf.get("DocCount"));
			Configuration conf = context.getConfiguration();
			String betaString = conf.get("beta").toString();
			LOG.info("BETASTRING "+betaString); //31
				
			String line = lineText.toString();
			String[] array = line.split(",");
			int len = array.length; //30
			
			if(line.contains("V1")){
				//skip the 1st row
				LOG.info("skipped");
			}
			else{
				//LOG.info("betastring "+betaString);
				double y_i = Double.parseDouble(array[len-1]);
				double residual;
				if(betaString.equals("invalid")){
					//first iteration
					prediction = 0.5;
					
				}
				else{
					
					String[] coefArray = betaString.split("%%%%%");
					int l = coefArray.length;
					LOG.info("len of coefArray > "+l);
					LOG.info("len of lineArray > "+len);
					double sum = Double.parseDouble(coefArray[0]);
					for(int i=0;i<len-1;i++){
						
						double x1 = Double.parseDouble(array[i]);
						double b1 = Double.parseDouble(coefArray[i+1]);
						sum = sum + (b1*x1);
						
						
					}
					//calculating prediction using logistic function
					LOG.info("map sum test else block.... "+sum);
					prediction = 1/(1+Math.exp(-sum));
					LOG.info("map prediction test else block.... "+prediction);
					
				}
				//calculating residual
				residual = prediction - y_i;
				LOG.info("residual "+residual);
				
				String s = String.valueOf(residual);
				boolean b = true;
				for(int i=0;i<len-1;i++){
					
					double x_i = Double.parseDouble(array[i]);
					//same residual for each column value x_i
					double d = residual * x_i;
					if(b){
						s = s + "#####";
					}
					s = s + String.valueOf(d);
					b = true;
					
				}
				
				context.write(new Text("beta"), new Text(s));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {
			double sum  = 0;
			double alpha = 0.3;
			ArrayList<ArrayList<Double>> arrayList = new ArrayList<>();
			
			boolean t = false;
			for ( Text count  : counts) {
				//(h_i - y_i)*x_i
				String s = count.toString();
				String[] arr = s.split("#####");
				LOG.info("sarr -> "+arr.length);
				
				if(!t){
					
					for(int i=0;i<arr.length;i++){ //30
						ArrayList<Double> list = new ArrayList<>();
						double d = Double.parseDouble(arr[i]);
						LOG.info("d !t ......"+d);
						list.add(d);	
						arrayList.add(list);
						LOG.info("if !t test..... "+list);
						
					}
				}
				else{
					
					for(int i=0;i<arr.length;i++){
						ArrayList<Double> list = new ArrayList<>();
						double d = Double.parseDouble(arr[i]);
						list.add(d);
						arrayList.get(i).add(d);
						
					}
					
				}
				t=true;
			}
			//double finalProduct = alpha * sum;
			
			Configuration conf = context.getConfiguration();
			String betaString = conf.get("beta");//31
			int c = Integer.parseInt(conf.get("columns"));//30
			int rows = Integer.parseInt(conf.get("rows"));
			LOG.info("rows -------test---- "+rows);
			
			
			if(betaString.equals("invalid")){
				sum=0.0;
				String newBetaString="";
				boolean b=false;
				LOG.info("arrofarr.size "+arrayList.size());
				LOG.info("columns "+c);
				for(int i=0;i<c;i++){
					
					// c because size of main arraylist will be equal to number of coefficients.
					// getting each arraylist from the Arraylist of arraylist  
					
					ArrayList<Double> list = new ArrayList<>();
					list = arrayList.get(i);
					for(int j=0;j<list.size();j++){
						sum = sum + list.get(j);
					}
					//new beta coefficient
					double finalProduct = (-1) * ((1/(double)rows) * alpha * sum);
					
					if(b){
						newBetaString = newBetaString+"%%%%%";
					}
					
					newBetaString = newBetaString + String.valueOf(finalProduct);
					b=true;
				}
				context.write(word, new Text(newBetaString));
				
			}
			else{
				//file contains - 
				//b0%%%%%b1%%%%%b2%%%%%b3%%%%%b4.......
				sum=0.0;
				String newBetaString="";
				boolean b=false;
				LOG.info("arrofarr.size else "+arrayList.size());
				LOG.info("columns else "+c);
				LOG.info("betaString else "+betaString);
				String[] betaArr = betaString.split("%%%%%");
				LOG.info("betaArr > "+ betaArr.length);
				for(int i=0;i<c;i++){
					//recently changed value of c
					// c because size of main arraylist will be equal to number of coefficients.
					// getting each arraylist from the Arraylist of arraylist  
					
					ArrayList<Double> list = new ArrayList<>();
					list = arrayList.get(i);	
					LOG.info("list --------- <> "+list);
					for(int j=0;j<list.size();j++){
						sum = (double)sum + list.get(j);
					}
					LOG.info("SUM test---------- "+sum);
					double b0 = Double.parseDouble(betaArr[i]);
					LOG.info("b0 test------- "+b0);
					
					
					//new coefficient
					double newb0 = b0 - ((1/(double)rows) * (alpha * sum));
					
					LOG.info("newb0 test------- "+newb0);
					
					if(b){
						newBetaString = newBetaString+"%%%%%";
					}
					
					newBetaString = newBetaString + String.valueOf(newb0);
					b=true;
				}
				
				context.write(word, new Text(newBetaString));
				
			}
			
			
		}
	}

}
