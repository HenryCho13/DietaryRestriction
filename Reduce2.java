package DietaryRestriction;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce2 
	extends Reducer<FeelCountPair, NullWritable, FeelCountPair, NullWritable> {
//	extends Reducer<Text, NullWritable, FeelCountPair, Text> {
	
	private String feel = "";
	private int n;
	
	public void reduce(FeelCountPair key, Iterable<NullWritable> values,
						Context context
						) throws IOException, InterruptedException {
		
//		for (NullWritable val: values) {
//			context.write(key, NullWritable.get());
//		}
		n = 0;
		for (NullWritable val: values) {
				if (n < 5) {
					context.write(key, NullWritable.get());
					n++;
				}
		}
	}
}
