package page_rank;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.net.URI; 
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(ParseMapper.class);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text Key = new Text();
        Text Value = new Text();
        Text Exist_tag = new Text();
	Exist_tag.set("EXIST_TAG");

        /*  Match title pattern */  
        String line = new String(unescapeXML(value.toString()));
        Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
        Matcher titleMatcher = titlePattern.matcher(line);
	if (titleMatcher.find()) {
	    String title = titleMatcher.group(1);
	    Value.set(title);
	}
        context.write(Value, Exist_tag);
        
        /*  Match link pattern */
        Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
        Matcher linkMatcher = linkPattern.matcher(line);
	while (linkMatcher.find()) {
	    Text tmp = new Text();
	    String correct = capitalizeFirstLetter(linkMatcher.group(1));
	    if (correct != ""){
	        Key.set(correct);
	        context.write(Key, Value);
	    }
	}
    }
    
    private String unescapeXML(String input) {

        return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'");

    }

    private String capitalizeFirstLetter(String input){

	char firstChar = 'A';
        if (input.length() > 0)
	    firstChar = input.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else 
                return input.substring(0, 1).toUpperCase() + input.substring(1);
	}
        else 
            return input;
    }
}

