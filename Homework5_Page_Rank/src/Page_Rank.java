package page_rank;

import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Page_Rank{
	public static enum PAGE_RANK_COUNTER {
	    N, D, DanglingSum, Error, Max, Min, Sum
        }
	
    	public static void main(String[] args) throws Exception {  
		//Job 1: Parse Pages
		System.out.println("========== Start Paringe Pages ==========");
		ParsePage parse_job = new ParsePage();
                parse_job.ParsePage(args);

		//Job 2: Calculate total numbers and append outgoings
		System.out.println("========== Start Appending Outgoings ==========");
		AppendOutgoing append_job = new AppendOutgoing();
		append_job.AppendOutgoing(args);
                int N = append_job.getN();
		
		//Job 3: Calculate page rank iteratively
		double Error_limit = 0.001;
		//double Error_limit = Double.parseDouble(args[6]);
		double Error = 1;
		long old_Error = 1;
		long new_Error = 1;
		int iter = Integer.parseInt(args[5]);
		int i = 0;
		for(; (iter > 0 && i<iter) || (iter == -1); i++){
		    System.out.println("========== Start Calculating Ranks ==========");
		    CalculateRank calculate_job = new CalculateRank();
                    args[0] = String.valueOf(N);
                    //args[1] = String.valueOf(new_Error);
                    calculate_job.CalculateRank(args);
		    old_Error = new_Error;
		    new_Error = calculate_job.getError();
		    //System.out.println("new_Error is: " +  String.valueOf(new_Error/1E18));
		    Error = Math.abs(new_Error - old_Error) / 1E18;
		    if (Error < Error_limit)
			break;
		    else if ((iter > 0 && (i+1) < iter) || (iter == -1)){
                        Configuration rank_conf = new Configuration();
			FileSystem FS = FileSystem.get(rank_conf);
			FS.delete(new Path(args[2]), true);
			FS.rename(new Path(args[3]), new Path(args[2]));
                    }           
		    System.out.println("Error is: " +  String.valueOf(Error));
		    System.out.println("iterations is: " +  String.valueOf(i));
		}
		System.out.println("Final Error is: " +  String.valueOf(Error));
		System.out.println("Final iterations is: " +  String.valueOf(i));

		//Job 4: Sort ranks
		System.out.println("========== Start Sorting Ranks ==========");
		SortRank sort_job = new SortRank();
                sort_job.SortRank(args);

		System.exit(0);
    	}  
}
