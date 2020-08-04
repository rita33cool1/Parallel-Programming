package page_rank;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.conf.Configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AppendReducer extends Reducer<Text, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(AppendReducer.class);
	
	private long N;

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job job = cluster.getJob(context.getJobID());
		N = job.getCounters().findCounter(Page_Rank.PAGE_RANK_COUNTER.N).getValue();
	}

	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    boolean isExist = false;
	    Set<String> hset = new LinkedHashSet<String>();
	    StringBuffer links = new StringBuffer();
	    double rank = (N == 0)? 0 : ((double)1) / N;
            links.append(String.valueOf(rank));
            int init_len = links.length();
	    for (Text val: values) {
	        if (!val.toString().equals("EXIST_TAG")){
                    links.append("|" + val.toString());
    	        }
	    }

            Text value = new Text(links.toString());
	    context.write(key, value);
	}
}
