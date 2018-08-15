package com.hadoop.reducer;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

/**
 * reducer类的测试类
 * @author carazheng
 *
 */
public class MaxTemperatureReducerTest {

	@Test
	public void returnsMaximumIntegerInValues() throws IOException
	{
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
			.withReducer(new MaxTemperatureReducer())
			.withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
			.withOutput(new Text("1950"), new IntWritable(10))
			.runTest();
	}
}
