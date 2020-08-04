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
import java.lang.Math;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CalculateMapper extends Mapper<Text, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(CalculateMapper.class);
    
    private int N;
    private double dangling_sum = 0;
    
    protected void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
        Cluster cluster = new Cluster(conf);
        Job job = cluster.getJob(context.getJobID());
        N = (int)job.getCounters().findCounter(Page_Rank.PAGE_RANK_COUNTER.N).getValue();
	context.getCounter(Page_Rank.PAGE_RANK_COUNTER.Error).setValue(0);
	context.getCounter(Page_Rank.PAGE_RANK_COUNTER.DanglingSum).setValue(0);
    }
    
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] split_out = value.toString().split("\\|");
	value.set("#" + value.toString());
        context.write(key, value);
        // Dangling node
	if (split_out.length == 1){
            dangling_sum += Double.parseDouble(split_out[0]);
	}
	// Not Dangling node
	else{
	    for (int i=1; i<(split_out.length); i++){
                key.set(split_out[i]);
		double rank = Double.parseDouble(split_out[0]) / (split_out.length-1);
		value.set(String.valueOf(rank));
		context.write(key, value);
	    }
	}
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    context.getCounter(Page_Rank.PAGE_RANK_COUNTER.DanglingSum).increment((long)(dangling_sum*1E18));
    }
    
}

