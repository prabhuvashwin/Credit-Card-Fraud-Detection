package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanSDMapReduce {
	
	// Mapper class takes the each row from the input file and splits it into a string array.
	// Mapper output will write the values as <Class>-<AttributeName>	<AttributeValue>. Each
	// attribute will be divided into 2 classes
	public static class MeanSDMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		public void map (LongWritable offset, Text lineText, Context context)
					throws IOException, InterruptedException {
			
			String[] line = lineText.toString().split("\\,");
			int i = 0;
			if (line[line.length - 1].equals("0")) {
				while ( i < 28 ) {
					context.write(new Text("Class0-V" + String.valueOf(i + 1)), new DoubleWritable(Double.valueOf(line[i])));
					i++;
				}
				context.write(new Text("Class0-Amount"), new DoubleWritable(Double.valueOf(line[i])));
			} else if (line[line.length - 1].equals("1")) {
				while ( i < 28 ) {
					context.write(new Text("Class1-V" + String.valueOf(i + 1)), new DoubleWritable(Double.valueOf(line[i])));
					i++;
				}
				context.write(new Text("Class1-Amount"), new DoubleWritable(Double.valueOf(line[i])));
			}
		}
	}
	
	// Reducer class will calculate the standard deviation value. The values for mean of each attributes
	// is calculated in the driver class after the completion of first MapReduce job
	public static class MeanSDReduce extends Reducer<Text, DoubleWritable, Text, Text> {
		
		Long numberOfDataPoints = (long) 0;
		Map<String, Double> mean = new HashMap<String, Double>();
		
		@Override
		public void setup (Context context) throws IOException, InterruptedException {
			
			numberOfDataPoints = Long.valueOf(context.getConfiguration().get("numberOfDataPoints").toString());
			for (int i = 1; i <= 28; i++) {
				String key = "Class0-V" + i;
				mean.put(key, Double.valueOf(context.getConfiguration().get(key).toString()) / numberOfDataPoints);
			}
			mean.put("Class0-Amount", Double.valueOf(context.getConfiguration().get("Class0-Amount").toString()) / numberOfDataPoints);
			
			for (int i = 1; i <= 28; i++) {
				String key = "Class1-V" + i;
				mean.put(key, Double.valueOf(context.getConfiguration().get(key).toString()) / numberOfDataPoints);
			}
			mean.put("Class1-Amount", Double.valueOf(context.getConfiguration().get("Class1-Amount").toString()) / numberOfDataPoints);
		}
		
		public void reduce (Text data, Iterable<DoubleWritable> values, Context context)
					throws IOException, InterruptedException {
			
			double SD = 0, temp = 0;
			
			for (DoubleWritable v : values) {
				temp += Math.pow(Double.valueOf(v.toString()) - mean.get(data.toString()), 2.0);
				
				SD = Math.sqrt(temp / numberOfDataPoints);
			}
			
			context.write(data, new Text(mean.get(data.toString()).toString() + "," + String.valueOf(SD)));
			
			
		}
	}
}
