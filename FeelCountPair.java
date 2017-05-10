package DietaryRestriction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

//Create composite {Feel, Count} key to implement secondary sort on count
public class FeelCountPair 
	implements Writable, WritableComparable<FeelCountPair> {

	String feel; 	//natural key
	String count;	//secondary key
	String food;
	
	public FeelCountPair() {
		
	}

	public FeelCountPair(String fe, String fo, String c) {
		feel = fe;
		food = fo;
		count = c;
	}

	public String getFeel() {
		return feel;
	}

	public void setFeel(String feel) {
		this.feel = feel;
	}

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
	}

	public String getFood() {
		return food;
	}

	public void setFood(String food) {
		this.food = food;
	}

	@Override
	public int compareTo(FeelCountPair pair) {
		int compareValue = this.feel.compareTo(pair.getFeel());
		if (compareValue == 0) {
			compareValue = count.compareTo(pair.getCount());
		}
		
		return -1*compareValue; 	//sort descending
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(feel).append(": ")
				.append(food).append(", ").append(count).append(" incidents")).toString();
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		feel = WritableUtils.readString(arg0);
		food = WritableUtils.readString(arg0);
		count = WritableUtils.readString(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeString(arg0, feel);
		WritableUtils.writeString(arg0, food);
		WritableUtils.writeString(arg0, count);
		
	}

}
