package info.craigmdupree;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable,Text,LongWritable,LongWritable>  {
	LongWritable outputKey = new LongWritable();
	LongWritable outputValue = new LongWritable();

	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		
		//put your logic 		
		String[] tokens = value.toString().split(",");
	
		 outputKey.set(Long.parseLong(tokens[0]));
		 outputValue.set(Long.parseLong(tokens[1]));
		
		context.write(outputKey, outputValue);
	}

}
