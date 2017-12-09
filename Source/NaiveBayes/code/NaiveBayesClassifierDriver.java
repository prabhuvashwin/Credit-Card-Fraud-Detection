package org.myorg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.myorg.InitialCountMapReduce.InitialCountMap;
import org.myorg.InitialCountMapReduce.InitialCountReduce;
import org.myorg.MeanSDMapReduce.MeanSDMap;
import org.myorg.MeanSDMapReduce.MeanSDReduce;
import org.myorg.NaiveBayesClassifierMapReduce.NaiveBayesClassifierMap;
import org.myorg.NaiveBayesClassifierMapReduce.NaiveBayesClassifierReduce;

// This class is the driver for Naive Bayes implementation
public class NaiveBayesClassifierDriver extends Configured implements Tool {

	public static void main(String[] args) throws IOException, InterruptedException, Exception {
		
		int res = ToolRunner.run(new NaiveBayesClassifierDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Path inputDirectory = new Path(args[0]);
		Path intermediateDirectory = new Path("intermediate");
		Path outputDirectory = new Path(args[1]);
		Path testInputPath = new Path(args[2]);
		ArrayList<String> testDataPoints = new ArrayList<String>();
		
		Configuration initialCountConf = new Configuration();
		
		// Remove the intermediate and output directories if present
		FileSystem fs = FileSystem.get(initialCountConf);
		try {
			if (fs.exists(outputDirectory))
				fs.delete(outputDirectory, true);
			if (fs.exists(intermediateDirectory))
				fs.delete(intermediateDirectory, true);
			
			fs.mkdirs(intermediateDirectory);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(testInputPath)));
			String line = br.readLine();
			while (line != null) {
				testDataPoints.add(line);
				line = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// This job calculates the sum of all the values of all the attributes in the input data, for each class.
		Job initialCountJob = Job.getInstance(initialCountConf, "InitialCount");
		initialCountJob.setJarByClass(this.getClass());
		FileInputFormat.setInputPaths(initialCountJob, inputDirectory);
		FileOutputFormat.setOutputPath(initialCountJob, new Path(intermediateDirectory, "initial_count"));
		initialCountJob.setMapperClass(InitialCountMap.class);
		initialCountJob.setReducerClass(InitialCountReduce.class);
		initialCountJob.setMapOutputKeyClass(Text.class);
		initialCountJob.setMapOutputValueClass(DoubleWritable.class);
		initialCountJob.setOutputKeyClass(Text.class);
		initialCountJob.setOutputValueClass(Text.class);
		int success = initialCountJob.waitForCompletion(true) ? 0 : 1;
		
		if (success == 1) return success;
		
		long dataCount = 0;
		Map<String, String> m = new HashMap<String, String>();
		try {
			String l;
			FileSystem fs1 = FileSystem.get(initialCountConf);
			Path tempPath = new Path(intermediateDirectory, "initial_count/part-r-00000");
			BufferedReader br = new BufferedReader(new InputStreamReader(fs1.open(tempPath)));
			
			while((l = br.readLine()) != null) {
				if (l.trim().length() > 0) {
					String[] parts = l.split("\\s+");
					m.put(parts[0], parts[1]);
				}
			}
			dataCount = Long.valueOf(m.get("Class0-Class0")) + Long.valueOf(m.get("Class1-Class1"));
			System.out.println("Class0: " + m.get("Class0-Class0"));
			System.out.println("Class1: " + m.get("Class1-Class1"));
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// This job calculates the mean and standard deviation of all the attributes is calculated. 
		// The input for this job is the input data file which we passed through the command line arguments, 
		// as well as the sum of all the values of all the attributes in the input data, which we calculated 
		// in the previous MapReduce Job.
		Job meanSDJob = Job.getInstance(getConf(), "MeanSD");
		Configuration meanSDConf = meanSDJob.getConfiguration();
		meanSDJob.setJarByClass(this.getClass());
		meanSDConf.set("numberOfDataPoints", String.valueOf(dataCount));
		for (int i = 1; i <= 28; i++) {
			String key = "Class0-V" + i;
			meanSDConf.set(key, m.get(key));
		}
		meanSDConf.set("Class0-Amount", m.get("Class0-Amount"));
		
		for (int i = 1; i <= 28; i++) {
			String key = "Class1-V" + i;
			meanSDConf.set(key, m.get(key));
		}
		meanSDConf.set("Class1-Amount", m.get("Class1-Amount"));
		
		FileInputFormat.setInputPaths(meanSDJob, inputDirectory);
		FileOutputFormat.setOutputPath(meanSDJob, new Path(intermediateDirectory, "mean_sd"));
		meanSDJob.setMapperClass(MeanSDMap.class);
		meanSDJob.setReducerClass(MeanSDReduce.class);
		meanSDJob.setMapOutputKeyClass(Text.class);
		meanSDJob.setMapOutputValueClass(DoubleWritable.class);
		meanSDJob.setOutputKeyClass(Text.class);
		meanSDJob.setOutputValueClass(Text.class);
		success = meanSDJob.waitForCompletion(true) ? 0 : 1;
		
		if (success == 1) return success;
		
		Map<String, String> mean = new HashMap<String, String>();
		Map<String, String> SD = new HashMap<String, String>();
		try {
			String l;
			FileSystem fs2 = FileSystem.get(meanSDConf);
			Path tempPath = new Path(intermediateDirectory, "mean_sd/part-r-00000");
			BufferedReader br = new BufferedReader(new InputStreamReader(fs2.open(tempPath)));
			
			while((l = br.readLine()) != null) {
				if (l.trim().length() > 0) {
					String[] parts = l.split("\\s+");
					mean.put(parts[0], parts[1].split("\\,")[0]);
					SD.put(parts[0], parts[1].split("\\,")[1]);
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// The next MapReduce job is where the Naïve Bayes algorithm is implemented, 
		// and the class for the input data is predicted. The input for this MapReduce job 
		// is the test data, and the mean and standard deviation values of all predictor variables.
		Job nbClassifierJob = Job.getInstance(getConf(), "NaiveBayesClassifier");
		Configuration nbClassifierConf = nbClassifierJob.getConfiguration();
		nbClassifierJob.setJarByClass(this.getClass());
		
		nbClassifierConf.set("numberOfDataPoints", String.valueOf(dataCount));
		for (int i = 1; i <= 28; i++) {
			String key = "Class0-meanV" + i;
			nbClassifierConf.set(key, mean.get("Class0-V" + i));
		}
		nbClassifierConf.set("Class0-meanAmount", mean.get("Class0-Amount"));
		
		for (int i = 1; i <= 28; i++) {
			String key = "Class1-meanV" + i;
			nbClassifierConf.set(key, mean.get("Class1-V" + i));
		}
		nbClassifierConf.set("Class1-meanAmount", mean.get("Class1-Amount"));
		
		for (int i = 1; i <= 28; i++) {
			String key = "Class0-SDV" + i;
			nbClassifierConf.set(key, SD.get("Class0-V" + i));
		}
		nbClassifierConf.set("Class0-SDAmount", SD.get("Class0-Amount"));
		
		for (int i = 1; i <= 28; i++) {
			String key = "Class1-SDV" + i;
			nbClassifierConf.set(key, SD.get("Class1-V" + i));
		}
		nbClassifierConf.set("Class1-SDAmount", SD.get("Class1-Amount"));
		nbClassifierConf.set("Class0-Class0", m.get("Class0-Class0"));
		nbClassifierConf.set("Class1-Class1", m.get("Class1-Class1"));
		
		nbClassifierConf.set("numberOfDataPoints", String.valueOf(dataCount));
		FileInputFormat.setInputPaths(nbClassifierJob, testInputPath);
		FileOutputFormat.setOutputPath(nbClassifierJob, outputDirectory);
		nbClassifierJob.setMapperClass(NaiveBayesClassifierMap.class);
		nbClassifierJob.setReducerClass(NaiveBayesClassifierReduce.class);
		nbClassifierJob.setMapOutputKeyClass(Text.class);
		nbClassifierJob.setMapOutputValueClass(Text.class);
		nbClassifierJob.setOutputKeyClass(Text.class);
		nbClassifierJob.setOutputValueClass(Text.class);
		success = nbClassifierJob.waitForCompletion(true) ? 0 : 1;
		
		FileSystem fs3 = FileSystem.get(initialCountConf);
		try {
			if (fs3.exists(intermediateDirectory))
				fs3.delete(intermediateDirectory, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return success;
	}

}
