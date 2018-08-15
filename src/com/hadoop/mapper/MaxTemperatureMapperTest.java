package com.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class MaxTemperatureMapperTest {

	@Test
	public void processesValidRecord() throws IOException
	{
		/**
		 * 传递一个天气记录作为mapper的输入，然后检查输出是否是读入的年份和气温。
		 */
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"//year
				+ "99999V0203201N00261220001CN9999999N9-00111+99999999999");//temperature
		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new MaxTemperatureMapper())
			.withInput(new LongWritable(0), value)
			.withOutput(new Text("1950"), new IntWritable(-11))
			.runTest();
	}
	/**
	 * 由于丢失气温的记录已经被过滤，所以这个测试使用Mockito来证实
	 * OutputCollector中的collect方法为调用任何Text键或IntWritable值。
	 * @throws IOException
	 */
	@Test
	public void ignoresMissingTemperatureRecord() throws IOException
	{
		MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"//year
				+ "99999V0203201N00261220001CN9999999N9+99991+99999999999");//temperature
		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new MaxTemperatureMapper())
			.withInput(new LongWritable(0), value)
			.runTest();
	}
}
