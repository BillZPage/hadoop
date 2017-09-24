package zl.hadoop;

import org.apache.hadoop.io.Text;


import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;


//mapper
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	//缺失
	private static final int MISSIONG = 9999;
	
	private InetAddress addr;
	private String hostname;
	private String ip;
	private SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	
	private long startTime;
	
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

	//设置函数，只调用一次
	protected void setup(Context context)
			throws IOException, InterruptedException {

		addr = InetAddress.getLocalHost();
		ip = addr.getHostAddress();
		hostname = addr.getHostName();

		startTime = System.currentTimeMillis();
		
		InputSplit split = context.getInputSplit();
		long length = split.getLength();
		
		System.out.println("split_len"+length+":"+hostname+":"+ip+":"+sdf.format(new Date())+":mapper:setup()");
		
		super.setup(context);
	}


	//清理，只调用一次
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		long duration = (System.currentTimeMillis() - startTime)/1000;
		System.out.println(hostname+":"+ip+":"+sdf.format(new Date())+":mapper:cleanup()"+duration);
		super.cleanup(context);
	}
	
	

}
