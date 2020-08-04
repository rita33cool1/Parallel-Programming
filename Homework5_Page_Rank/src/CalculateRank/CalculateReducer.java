package page_rank;

import java.util.*;
import java.lang.Math;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.conf.Configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CalculateReducer extends Reducer<Text, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(CalculateReducer.class);
	
	private int N;
        private double local_err = 0;
        private double ALPHA = 0.85;
        private double dangling_sum = 0;

	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    Cluster cluster = new Cluster(conf);
	    Job job = cluster.getJob(context.getJobID());
	    N = conf.getInt("N", 0);
	    dangling_sum = job.getCounters().findCounter(Page_Rank.PAGE_RANK_COUNTER.DanglingSum).getValue() / 1E18;	
	}

	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    String links = "";
	    double sum = 0;
	    double old_rank = 0;
	    boolean isDangling = true;
	    for (Text val: values) {
	        String[] split_out = val.toString().split("\\|");
	        char first_char = split_out[0].charAt(0);
		// Ingoing node' rank
	        if (split_out.length == 1 && first_char != '#'){
                    sum += Double.parseDouble(split_out[0]);
	        }
		// Danglin nodes
		else if (split_out.length == 1){
		    String[] tokens = val.toString().split("#", 2);
                    old_rank = Double.parseDouble(tokens[1]);
		    links = tokens[1];
		}
		else{
		    isDangling = false;
		    String[] tokens1 = val.toString().split("#", 2);
		    String[] tokens2 = tokens1[1].toString().split("\\|", 2);
                    old_rank = Double.parseDouble(tokens2[0]);
		    links = tokens2[1];
		}
	    }
	    double rank = (1-ALPHA)/N + ALPHA*sum + ALPHA*dangling_sum/N;
	    Text value = new Text();
	    if (isDangling)
	       value.set(String.valueOf(rank));
	    else
	       value.set(String.valueOf(rank) + "|" + links);
	    context.write(key, value);

	    local_err += Math.abs(rank - old_rank);
        }
        
	protected void cleanup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    context.getCounter(Page_Rank.PAGE_RANK_COUNTER.Error).increment((long)(local_err*1E18));
	    
	}
}
