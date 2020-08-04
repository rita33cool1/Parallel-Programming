package page_rank;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ParseReducer extends Reducer<Text, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(ParseReducer.class);
	// Combiner implements method in Reducer
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean isExist = false;
		String EXIST_TAG = new String("EXIST_TAG");
		Set<String> hset = new LinkedHashSet<String>();
		Text tmp = new Text();
		int i = 0;
		for (Text val: values) {
		    if (val.toString().equals(EXIST_TAG)){
			isExist = true;
                        hset.add(val.toString());
		    }
		    else 
                        hset.add("&" + String.valueOf(i) + "&" + val.toString());
	            i++;
		}

                Iterator iterator = hset.iterator();
		i = 0;
		if (isExist && (!key.equals(new String("")))){
		    while (iterator.hasNext()){
			i++;
			String element = new String(iterator.next().toString());
		        char firstChar = element.charAt(0);
			if (firstChar == '&'){
			    element = element.split("&", 3)[2]; 
			    tmp.set(element);
                            context.write(tmp, key);
			}
			else{
                            tmp.set(EXIST_TAG);
                            context.write(key, tmp);
			}   
		    }
		}
	}
}
