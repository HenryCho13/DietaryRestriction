package DietaryRestrictions;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DietaryRestrictions {
	
	public static class FeelFoodMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException
		{
			String type;
			String typeValue;
			String dateTime;

			String[] row = value.toString().split("\n");
			ArrayList<String> foodList = new ArrayList<String>();

			for (int i = 0; i < row.length; i++)
			{
				String line = row[i];
				String[] parseLine = line.split(" ");
				type = parseLine[0];
				typeValue = parseLine[1];
				dateTime = parseLine[2] = parseLine[3];
				
				if (type == "food")
				{
					
				}
				else
				{
					
				}
				
			}
			
		}
	}

	public static class FeelFoodReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Text values, Context context)
				throws IOException, InterruptedException
		{
			
		}
	}
	
//	public class Food
//	{
//		String foodName;
//		Date date;
//		
//		
//		public String getFoodName() {
//			return foodName;
//		}
//
//		public void setFoodName(String foodName) {
//			this.foodName = foodName;
//		}
//
//		public Date getDate() {
//			return date;
//		}
//
//		public void setDate(Date date) {
//			this.date = date;
//		}
//		
//		Food()
//		{
//			foodName = "";
//			date = null;
//		}
//		
//	}
}
