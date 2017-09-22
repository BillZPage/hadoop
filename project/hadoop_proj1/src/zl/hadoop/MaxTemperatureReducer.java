package zl.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer¿‡
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	//reduce
	protected void reduce(Text keyin, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int maxValue = Integer.MIN_VALUE;
		for(IntWritable value : values)
		{
			maxValue = Math.max(maxValue, value.get());
		}
		
		//REDUCE ‰≥ˆ
		context.write(keyin, new IntWritable(maxValue));
	}

	
}
