import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// Calculate total number eligible for pay rise

public class Program1 {

	//MAPPER CODE	
		   
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	// the variable one holds the value 1 in the form of Hadoop's IntWritable Object
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String myvalue = value.toString();
		String[] tokens = myvalue.split(",");
		if(tokens[5].matches("YES")) {
			output.collect(new Text(tokens[5]), one);
		}
		}
	}

	//REDUCER CODE	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
			int transcount = 0;
			while(values.hasNext()) {
				transcount += values.next().get();
			}
			
			output.collect(new Text("Eligible for Pay Raise"), new IntWritable(transcount));
		}
	}
		
	//DRIVER CODE
	public static void main(String[] args) throws Exception {
		
		// JobConf is a constructor and is used to construct a map/reduce job configuration.
		JobConf conf = new JobConf(Program1.class);
		conf.setJobName("Various Operations");
		
		//Set the key class for the job output data.
		// Text class is part of hadoop API
		conf.setOutputKeyClass(Text.class);
		
		// Set the value class for job outputs 
		conf.setOutputValueClass(IntWritable.class);
		
		// set the mapper class
		conf.setMapperClass(Map.class);
		
		// set the combiner class
		conf.setCombinerClass(Reduce.class);
		
		// set the reducer class
		conf.setReducerClass(Reduce.class);
		
		// set input format
		conf.setInputFormat(TextInputFormat.class);
		
		//set output format
		conf.setOutputFormat(TextOutputFormat.class); 
		
		// set the first argument as input
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		
		//set the second argument as output
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		// run the job
		JobClient.runJob(conf);   
	}
}