package page_rank;

import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

public class CalculateRank{
        private static final int reducer_num = 30;
	
        private long Error;
        private Configuration conf;

	public CalculateRank(){

  	}
	
	public void CalculateRank(String[] args) throws Exception {
		conf = new Configuration();
                int N = Integer.parseInt(args[0]);
		conf.setInt("N", N);		
		Job job = Job.getInstance(conf, "CalculateRank");
		job.setJarByClass(CalculateRank.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(CalculateMapper.class);
		job.setPartitionerClass(CalculatePartitioner.class);
		//job.setCombinerClass(CalculateCombiner.class);
		job.setReducerClass(CalculateReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer

		job.setNumReduceTasks(reducer_num);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.waitForCompletion(true);
		Error = job.getCounters().findCounter(Page_Rank.PAGE_RANK_COUNTER.Error).getValue();
	}
        
	public long getError(){
	    return Error;
	}
	
	public Configuration getConf(){
	    return conf;
	}
	
	
}
