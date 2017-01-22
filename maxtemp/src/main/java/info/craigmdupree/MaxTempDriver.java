package info.craigmdupree;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import info.craigmdupree.MaxTempDriver;
import info.craigmdupree.MaxTempMapper;
import info.craigmdupree.MaxTempReducer;

public class MaxTempDriver {
	
public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Maximum Temp Job");
    
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class); // Not sure here. Before we were output a LW, Text
    
    String input=args[0];
    String outpath = args[1];
    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(outpath));
    
    
    job.setJarByClass(MaxTempDriver.class);
    job.setMapperClass(MaxTempMapper.class);
    job.setReducerClass(MaxTempReducer.class);
    
    
    job.waitForCompletion(true);
    
    
}

}
