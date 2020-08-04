package page_rank;

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

public class AppendOutgoing{
        private static final int reducer_num = 30;
	
        private int N = 0;

	public AppendOutgoing(){
  	}
	
	public void AppendOutgoing(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "AppendOutgoing");
		job.setJarByClass(AppendOutgoing.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(AppendMapper.class);
		job.setPartitionerClass(AppendPartitioner.class);
		//job.setCombinerClass(AppendCombiner.class);
		job.setReducerClass(AppendReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer

		job.setNumReduceTasks(reducer_num);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
		N = (int)job.getCounters().findCounter(Page_Rank.PAGE_RANK_COUNTER.N).getValue();
	}

	public int getN(){
	    return N;
	}
}
