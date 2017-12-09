package com.cloud.lrgd;

import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;



public class Driver  extends Configured implements Tool {
	
	static String inputDirectory;
	static String countDirectory;
	static String outputDirectory;
	static String testDataDirectory;
	
	public static void main(String[] args) throws Exception{
		
		int res = ToolRunner.run(new Driver(), args);
		System.exit(res);
		
	}  
	
	public int run(String[] args) throws Exception {
		
		inputDirectory = args[0];
		countDirectory = args[1];
		outputDirectory = args[2];
		testDataDirectory = args[3];
		
		
		//running the CountLines job for calculating the total number of lines in a file
		int isCounted = ToolRunner.run(new CountLines(), args);
		if (isCounted==1){
			return 1;
		}
		
		//iterating 
		String[] args1 = new String[4];
		args1[0] = inputDirectory; 		// i/p location of .csv file
		//args1[1] = 				 	// i/p location of file having beta coefficients
		//args1[2] = 					// o/p location of file having beta coefficients
		args1[3] = countDirectory; 			// location of file having count of rows and columns of .csv file
		for(int i=0 ; i < 100 ; i++){
			
			Log.info("==================== Iteration "+i+" ==========================");
			
			if(i==0){
				//1st iteration
				args1[1] = "iteration1";
			}
			else{
				//other iteration
				args1[1] = outputDirectory+"/iteration"+new DecimalFormat("00").format(i);
				
			}
			args1[2] = outputDirectory+"/iteration"+new DecimalFormat("00").format(i+1);
			
			//running the CalculateLR for calculating new beta values in each iteration
			int res = ToolRunner.run(new CalculateLR(), args1);
			if (res==1){
				return 1;
			}
			
			
		}
		
		Log.info("Starting Prediction Phase using Test data");
		
		args1[0] = testDataDirectory;						// -> input test file
		args1[1] = outputDirectory+"/finalOutput";			// -> final output path for prediction
		args1[2] = args1[2];								// -> final o/p directory of beta coefficients
		args1[3] = countDirectory;							// -> location of file having count of rows and columns of .csv file
		
		//running the PredictLR for prediction on test data
		int isPredicted = ToolRunner.run(new PredictLR(), args1);
		if (isPredicted==1){
			return 1;
		}
		
		args1[0] = args1[1];
		args1[1] = outputDirectory+"/accuracy";
		
		//Running the check accuracy for getting True Positives, True Negatives, False Positives, False Negatives
		int isAccurate = ToolRunner.run(new CheckAccuracy(), args1);
		if (isAccurate==1){
			return 1;
		}
		return 0;
		
		
	}
}
