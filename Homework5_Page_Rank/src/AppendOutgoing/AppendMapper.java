package page_rank;
import java.io.IOException;
import org.apache.hadoop.io.Text;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.net.URI; 
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AppendMapper extends Mapper<Text, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(AppendMapper.class);
    
    private HashSet<String> titles;

    protected void setup(Context context) throws IOException, InterruptedException {
	titles = new HashSet<String>();
    }
    
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

	if (value.toString().equals("EXIST_TAG")){
            titles.add(key.toString());
	}
	context.write(key, value);

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    Cluster cluster = new Cluster(conf);
	    Job job = cluster.getJob(context.getJobID());
	    context.getCounter(Page_Rank.PAGE_RANK_COUNTER.N).increment(titles.size());
	    long N = job.getCounters().findCounter(Page_Rank.PAGE_RANK_COUNTER.N).getValue();
	}
    
}

