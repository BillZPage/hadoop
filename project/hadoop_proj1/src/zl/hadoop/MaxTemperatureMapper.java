package zl.hadoop;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


//mapper
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	//缺失
	private static final int MISSIONG = 9999;
	
	//MAP方法
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//转换text类到string类型
		String line = value.toString();
		
		//年份数据
		String year = line.substring(15,19);
		
		int airTemperature;
		if(line.charAt(87) == '+')
		{
			airTemperature = Integer.parseInt(line.substring(88, 92));
		}
		else
		{
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		
		String quality = line.substring(92, 93);
		if(airTemperature != MISSIONG && quality.matches("[01459]"))
		{
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}

}
