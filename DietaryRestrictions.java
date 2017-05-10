package DietaryRestriction;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DietaryRestrictions {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("fs.default.name", "hdfs://localhost:9000");
		    
	    Job job1 = Job.getInstance(conf, "DietaryRestrictions");
	    job1.setJarByClass(DietaryRestrictions.class);
	    job1.setMapperClass(Map1.class);
	    job1.setCombinerClass(Reduce1.class);
	    job1.setReducerClass(Reduce1.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job1, new Path("input"));
	    FileOutputFormat.setOutputPath(job1, new Path("output-1"));
	    
	    boolean success = job1.waitForCompletion(true);
	    
	    if (success) {
		    Job job2 = Job.getInstance(conf, "DietaryRestrictions");
		    job2.setJarByClass(DietaryRestrictions.class);
		    job2.setMapperClass(Map2.class);
		    job2.setCombinerClass(Reduce2.class);
		    job2.setReducerClass(Reduce2.class);
		    job2.setOutputKeyClass(FeelCountPair.class);
		    job2.setOutputValueClass(NullWritable.class);
//		    job2.setOutputValueClass(Text.class);
		    job2.setPartitionerClass(FeelCountPartitioner.class);
		    job2.setGroupingComparatorClass(FeelCountGroupingComparator.class);
		    FileInputFormat.addInputPath(job2, new Path("output-1"));
		    FileOutputFormat.setOutputPath(job2, new Path("output-2"));
		    success = job2.waitForCompletion(true);
	    }
	   
	  }

}
