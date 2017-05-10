package DietaryRestriction;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce2 
	extends Reducer<Text, NullWritable, FeelCountPair, NullWritable> {
//	extends Reducer<Text, NullWritable, FeelCountPair, Text> {
	
	private Text feelFood;
	private Text incidents;
	
	public void reduce(FeelCountPair key, Iterable<NullWritable> values,
						Context context
						) throws IOException, InterruptedException {
	
//	public void reduce(FeelCountPair key, Iterable<Text> values,
//			Context context
//			) throws IOException, InterruptedException {
	
		
	}
}
