package com.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 求最大气温的reduce操作类
 * @author carazheng
 *
 */
public class MaxTemperatureReducer 
	extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, 
			Iterable<IntWritable> values, 
			Context context) throws IOException, InterruptedException {
		int maxValue = Integer.MIN_VALUE;
		for(IntWritable value:values)
		{
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}

	
}
