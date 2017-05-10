package DietaryRestriction;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map1 
	extends Mapper<Object, Text, Text, IntWritable> { 	

	private Text feelFood = new Text();
	private Text test = new Text();
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
	
		else if (type.equals("feel")) {
			for (int i=0; i < foodList.size(); i++) {
				long diff = date.getTime() - foodList.get(i).getDate().getTime();
				long diffHours = diff / (60 * 60 * 1000);
				
				
				if (diffHours <= 12) {
					feelFood.set(typeValue + "\t" + foodList.get(i).getValue());
					context.write(feelFood, one);
				}
				
				else {
					foodList.remove(i);
	          }
	        }
		}
	}
	
	private class Food {
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
}

