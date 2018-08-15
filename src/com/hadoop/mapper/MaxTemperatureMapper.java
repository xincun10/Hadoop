package com.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 求最大气温的map操作类
 * @author carazheng
 *
 */
public class MaxTemperatureMapper extends
	Mapper<LongWritable, Text, Text, IntWritable>{

	/**
	 * 将年份和气温值从行中读取出来并传到OutputColleactor
	 * @throws InterruptedException 
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);
		String temp = line.substring(87, 92);
		if(!missing(temp))
		{
			int airTemperature = Integer.parseInt(temp);
			context.write(new Text(year), new IntWritable(airTemperature));
		}		
	}
	
	//判断气温记录是否丢失
	private boolean missing(String temp)
	{
		return temp.equals("+9999");
	}

}
