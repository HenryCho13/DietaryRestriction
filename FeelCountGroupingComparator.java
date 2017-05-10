package DietaryRestriction;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Comparator controls which keys are grouped together
//for a single call to reduce function

public class FeelCountGroupingComparator 
	extends WritableComparator {
	
	public FeelCountGroupingComparator() {
		super(FeelCountPair.class, true);
	}
	
	@Override 
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		FeelCountPair pair = (FeelCountPair) wc1;
		FeelCountPair pair2 = (FeelCountPair) wc2;
		return pair.getFeel().compareTo(pair2.getFeel());

	}
}
