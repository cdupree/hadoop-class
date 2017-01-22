package info.craigmdupree;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MaxTempReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
	LongWritable outputKey = new LongWritable();
	LongWritable outputValue = new LongWritable();
	
	protected void reduce(LongWritable key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {

		//put logic
		
		// gets a longWritable like 1900 as key which goes right out as outputKey
		// and gets a longWritable Array of values like [t1, t2, ... tn] from which we pull out the
		// max value
		
		outputKey = key;
		
		Long max = 0L;
		for (LongWritable val: values) {
			Long lval = val.get();
			
			if (lval > max) 
				max = lval;
		}
		
		outputValue.set(new Long(max));
		
		context.write(outputKey, outputValue);
		
	}

}
