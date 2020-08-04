package page_rank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AppendPartitioner extends Partitioner<Text, Text> {
    // Debug
    private static final Log LOG = LogFactory.getLog(AppendPartitioner.class);
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
	char firstChar = key.toString().charAt(0);

        if ( firstChar >= 'A' && firstChar <= 'Z'){
	    return (firstChar - 'A');
	}
	else if ( firstChar == '1' )
	    return 26;
	else if ( firstChar == '2' )
	    return 27;
	else if ( firstChar >= '0' && firstChar <= '9')
	    return 28;
	else
	    return 29;
	
    }
}
