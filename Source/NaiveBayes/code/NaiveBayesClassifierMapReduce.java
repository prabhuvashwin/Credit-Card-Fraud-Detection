package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class NaiveBayesClassifierMapReduce {
	
	// Mapper class takes in the test dataset and will calculate the posterior probability of class 0 and
	// class 1. 
	public static class NaiveBayesClassifierMap extends Mapper<LongWritable, Text, Text, Text> {
		
		Map<String, Double> testValues = new HashMap<String, Double>();
		double numberOfDataPoints = 0;
		Map<String, Double> mean = new HashMap<String, Double>();
		Map<String, Double> SD = new HashMap<String, Double>();
		double numberOfClass0 = 0, numberOfClass1 = 0;
		Logger log = Logger.getLogger(this.getClass());
		
		@Override
		public void setup (Context context) throws IOException, InterruptedException {
			
			numberOfDataPoints = Double.valueOf(context.getConfiguration().get("numberOfDataPoints").toString());
			numberOfClass0 = Double.valueOf(context.getConfiguration().get("Class0-Class0").toString());
			numberOfClass1 = Double.valueOf(context.getConfiguration().get("Class1-Class1").toString());
			log.info("Class0: " + String.valueOf(numberOfClass0));
			log.info("Class1: " + String.valueOf(numberOfClass1));
			
			for (int i = 1; i <= 28; i++) {
				String key = "Class0-meanV" + i;
				mean.put(key, Double.valueOf(context.getConfiguration().get(key).toString()));
			}
			mean.put("Class0-meanAmount", Double.valueOf(context.getConfiguration().get("Class0-meanAmount").toString()));
			
			for (int i = 1; i <= 28; i++) {
				String key = "Class1-meanV" + i;
				mean.put(key, Double.valueOf(context.getConfiguration().get(key).toString()));
			}
			mean.put("Class1-meanAmount", Double.valueOf(context.getConfiguration().get("Class1-meanAmount").toString()));
			
			for (int i = 1; i <= 28; i++) {
				String key = "Class0-SDV" + i;
				SD.put(key, Double.valueOf(context.getConfiguration().get(key).toString()));
			}
			SD.put("Class0-SDAmount", Double.valueOf(context.getConfiguration().get("Class0-SDAmount").toString()));
			
			for (int i = 1; i <= 28; i++) {
				String key = "Class1-SDV" + i;
				SD.put(key, Double.valueOf(context.getConfiguration().get(key).toString()));
			}
			SD.put("Class1-SDAmount", Double.valueOf(context.getConfiguration().get("Class1-SDAmount").toString()));
		}
		
		public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
			
			
			String[] line = lineText.toString().split("\\,");
			int i = 0;
			while ( i < 28 ) {
				testValues.put("V" + String.valueOf(i + 1), Double.valueOf(line[i]));
				i++;
			}
			testValues.put("Amount", Double.valueOf(line[i]));
			
			double conditionalProbabilityWithClass0 = 1.0, conditionalProbabilityWithClass1 = 1.0;
			
			for (String t : testValues.keySet()) {
				double temp = ProbabilityDensityFunction(t, "Class0", testValues.get(t));
				conditionalProbabilityWithClass0 = conditionalProbabilityWithClass0 * temp;
			}
			
			for (String t : testValues.keySet()) {
				double temp = ProbabilityDensityFunction(t, "Class1", testValues.get(t));
				conditionalProbabilityWithClass1 = conditionalProbabilityWithClass1 * temp;
			}
			
			double probClass0 = conditionalProbabilityWithClass0 * (numberOfClass0 / numberOfDataPoints);
			double probClass1 = conditionalProbabilityWithClass1 * (numberOfClass1 / numberOfDataPoints);
			log.info("probClass0: " + String.valueOf(probClass0));
			log.info("probClass1: " + String.valueOf(probClass1));
			
			
			if (probClass1 > probClass0) context.write(lineText, new Text("1"));
			else context.write(lineText, new Text("0"));
		}
		
		// Probability Density function for gaussian distribution
		private double ProbabilityDensityFunction(String key, String c, double v) {
			
			String meank = c + "-mean" + key;
			String SDk = c + "-SD" + key;
			double value1 = 1 / Math.sqrt(2 * Math.PI * Math.pow(SD.get(SDk), 2));
			//log.info(key + " Value1: " + String.valueOf(value1));
			double value2 = Math.exp((-1 * (Math.pow(v - mean.get(meank), 2)) / (2 * Math.pow(SD.get(SDk), 2))));
			//log.info(key + " Value2: " +  String.valueOf(value2));
			//log.info(key + " Value1 * Value2: " + String.valueOf(value1 * value2));
			return value1 * value2;
		}
	}
	
	// Reducer class will write the prediction for each test data point
	public static class NaiveBayesClassifierReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			for (Text t : values) {
				context.write(key, t);
			}
		}
	}
}
