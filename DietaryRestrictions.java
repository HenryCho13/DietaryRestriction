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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DietaryRestrictions {
	
	public class Map1 
		extends Mapper<Object, Text, Text, IntWritable> { 	

	    private Text feelFood = new Text();
	    private final IntWritable one = new IntWritable(1);
	    ArrayList<Food>foodList = new ArrayList<Food>();

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy/M/d H:mm");
			String type;
			String typeValue;
			String dateTime;

			String[] row = value.toString().split(" ");
			
			type = row[0];
			typeValue = row[1];
			dateTime = row[3] + " " + row[4];
			Date date = null;
			
			try {
				date = formatter.parse(dateTime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			if (type.equals("food")) {
				Food food = new Food(typeValue, date);	//convert date and time to ms 	
				foodList.add(food);
			}
          
	    	else if (type.equals("feeling")) {
		        for (int i=0; i < foodList.size(); i++) {
		          if (date.getTime() - foodList.get(i).getDate().getTime() <= 12) {
		        	  feelFood.set("type" + " " + foodList.get(i).getValue());
		        	  context.write(feelFood, one);
		          }
		          else {
		        	  foodList.remove(i);
		          }
		        }
	    	}
		}
	}
	
	public static class Reduce1
	extends Reducer<Text,IntWritable,Text, IntWritable> {
		
		private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }

	}
	
	
	public class Food {
		String value;
		Date date;
		
		Food(String v, Date d) {
			date = d;
			value = v;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public Date getDate() {
			return date;
		}

		public void setDate(Date date) {
			this.date = date;
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("fs.default.name", "hdfs://localhost:9000");
		    
	    Job job = Job.getInstance(conf, "DietaryRestrictions");
	    job.setJarByClass(DietaryRestrictions.class);
	    job.setMapperClass(Map1.class);
	    job.setCombinerClass(Reduce1.class);
	    job.setReducerClass(Reduce1.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("input"));
	    FileOutputFormat.setOutputPath(job, new Path("output"));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    System.out.println("Job Completed");
	  }

}
