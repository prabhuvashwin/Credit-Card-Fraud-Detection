package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class InitialCountMapReduce {
	
	// Mapper class takes the each row from the input file and splits it into a string array.
	// Mapper output will write the values as <Class>-<AttributeName>	<AttributeValue>. Each
	// attribute will be divided into 2 classes
	public static class InitialCountMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		public final static IntWritable one = new IntWritable(1);
		Logger log = Logger.getLogger(this.getClass());
		
		public void map (LongWritable offset, Text lineText, Context context)
					throws IOException, InterruptedException {
			
			String[] line = lineText.toString().split("\\,");
			
			int i = 0;
			log.info(line[line.length - 1]);
			if (line[line.length - 1].equals("0")) {
				while ( i < 28 ) {
					context.write(new Text("Class0-V" + String.valueOf(i + 1)), new DoubleWritable(Double.valueOf(line[i])));
					i++;
				}
				context.write(new Text("Class0-Amount"), new DoubleWritable(Double.valueOf(line[i]))); i++;
				context.write(new Text("Class0-Class" + line[i]), new DoubleWritable(1.0));
			} else if (line[line.length - 1].equals("1")) {
				while ( i < 28 ) {
					context.write(new Text("Class1-V" + String.valueOf(i + 1)), new DoubleWritable(Double.valueOf(line[i])));
					i++;
				}
				context.write(new Text("Class1-Amount"), new DoubleWritable(Double.valueOf(line[i]))); i++;
				context.write(new Text("Class1-Class" + line[i]), new DoubleWritable(1.0));
			}
		}
	}

	// Reducer class will find the sum of all attributes under each class
	public static class InitialCountReduce extends Reducer<Text, DoubleWritable, Text, Text> {
		
		public void reduce (Text data, Iterable<DoubleWritable> counts, Context context)
					throws IOException, InterruptedException {
			
			int count = 0;
			
			for (DoubleWritable c : counts) {
				count += c.get();
			}
			
			context.write(data, new Text(String.valueOf(count)));
		}
	}
}
