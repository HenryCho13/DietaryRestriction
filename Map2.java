package DietaryRestriction;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map2 
//	extends Mapper<Object, Text, FeelCountPair, NullWritable> {
	extends Mapper<Object, Text, FeelCountPair, NullWritable> {
	
	private FeelCountPair compositeKey;
	private Text foodCount = new Text();
	
	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
		
		String feelValue = null;
		String food = null;
		String count = null;
		
		String[] row = value.toString().split("\t");
		feelValue = row[0];
		food = row[1];
		count = row[2];

		compositeKey = new FeelCountPair();
		compositeKey.setFeel(feelValue);
		compositeKey.setCount(count);
		compositeKey.setFood(food);
		
		foodCount.set(count);
		context.write(compositeKey, NullWritable.get());
	}
}
