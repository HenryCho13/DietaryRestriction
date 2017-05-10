package DietaryRestriction;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Sorts by incident count descending
public class FeelCountKeySortComparator 
	extends WritableComparator {
	
	protected FeelCountKeySortComparator() {
		super(FeelCountPair.class, true);
	}
	
	@Override 
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		FeelCountPair pair = (FeelCountPair) wc1;
		FeelCountPair pair2 = (FeelCountPair) wc2;
		
		IntWritable pairCount;
		IntWritable pair2Count;
		
		pairCount = new IntWritable(Integer.parseInt(pair.getCount()));
		pair2Count = new IntWritable(Integer.parseInt(pair2.getCount()));
		
		int compare = pair.getFeel().compareTo(pair2.getFeel());
		
		//Same feel
		if (compare == 0) {	
			return -pairCount
					.compareTo(pair2Count);
		}
		
		return compare;

	}
	
}
