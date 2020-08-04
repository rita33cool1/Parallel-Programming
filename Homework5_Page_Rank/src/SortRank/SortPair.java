package page_rank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class SortPair implements WritableComparable{
	private Text word;
	private double average;

	public SortPair() {
		word = new Text();
		average = 0.0;
	}

	public SortPair(Text word, double average) {
		this.word = word;
		this.average = average;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.word.write(out);
		out.writeDouble(this.average);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.word.readFields(in);
		this.average = in.readDouble();
	}

	public Text getWord() {
		return this.word;
	}

	public double getAverage() {
		return this.average;
	}

	@Override
	public int compareTo(Object o) {

		double thisAverage = this.getAverage();
		double thatAverage = ((SortPair)o).getAverage();

		Text thisWord = this.getWord();
		Text thatWord = ((SortPair)o).getWord();

		String thisStr = thisWord.toString();
		String thatStr = thatWord.toString();

                if (thisAverage == thatAverage ){
		    for (int i=0; i<thisStr.length() && i<thatStr.length(); i++){
			if ((int)thisStr.charAt(i) == (int)thatStr.charAt(i)){
                            continue;
			}
			return ((int)thisStr.charAt(i) < (int)thatStr.charAt(i) ? -1: 1);
		    }
		    return 0;
		}
		else
		    return (thisAverage > thatAverage ? -1 : 1);
	}
} 
