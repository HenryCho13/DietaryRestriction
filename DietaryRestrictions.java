package DietaryRestriction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DietaryRestrictions {
	
	public static class FeelFoodMapper 
		extends Mapper<Object, Text, Text, Text> { 	

	    private Text datetime = new Text();
	    private Text keyvalue = new Text();

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String type;
			String typeValue;
			String date;
			String time;

			String[] row = value.toString().split(" ");
			
			type = row[0];
			typeValue = row[1];
			date = row[3];
			time = row[4];

			datetime.set(date + " " + time);
			keyvalue.set(type + ": " + typeValue);
			context.write(datetime, keyvalue);

		}
	}
	
	public static class FeelFoodReducer
      extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterator<Text> values, Context context) 
				throws IOException, InterruptedException {
		
			
//			for (int i=values.getLength()-1; i<=0; i--) {
//				context.write(key, values.);
//			}
			
		}
	}
	
//	public static class FeelFoodReducer extends Reducer<Text, Text, Text, Text>
//	{
//		public void reduce(Text key, Text values, Context context)
//				throws IOException, InterruptedException
//		{
//			
//		}
//	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("fs.default.name", "hdfs://localhost:9000");
		    
	    Job job = Job.getInstance(conf, "DietaryRestrictions");
	    job.setJarByClass(DietaryRestrictions.class);
	    job.setMapperClass(FeelFoodMapper.class);
	    job.setCombinerClass(FeelFoodReducer.class);
	    job.setReducerClass(FeelFoodReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("input.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("output"));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    System.out.println("Job Completed");
	  }

}
