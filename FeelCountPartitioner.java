package DietaryRestriction;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//FeelCountPartitioner ensures that all data with the same natural key (feel) 
//is sent to the same reducer. 

public class FeelCountPartitioner 
//	extends Partitioner <FeelCountPair, NullWritable> {
	extends Partitioner <FeelCountPair, Text> {

	@Override
	public int getPartition(FeelCountPair pair, Text value, int numOfParitions) {
	//public int getPartition(FeelCountPair pair, NullWritable value, int numOfParitions) {
		//Paritions are non-negative 
		return Math.abs(pair.getFeel().hashCode() % numOfParitions);
	}

}
