package page_rank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SortMapper extends Mapper<Text, Text, SortPair, NullWritable> {
	
	private static final Log LOG = LogFactory.getLog(SortMapper.class);
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		double average = Double.parseDouble(value.toString().split("\\|")[0]);
                SortPair sp = new SortPair(key,average);
		//Text tmp = new Text();
                //tmp.set("page: " + String.valueOf(sp.getWord()));
		//LOG.info(tmp);
		//tmp.set("value: " + String.valueOf(sp.getAverage()));
		//LOG.info(tmp);
		context.write(sp, NullWritable.get());
	}
	
}
