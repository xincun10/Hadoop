package com.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.hadoop.mapper.MaxTemperatureMapper;
import com.hadoop.reducer.MaxTemperatureReducer;

public class MaxTemperatureDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
//		if(args.length!=2)
//		{
//			System.err.printf("Usage:%s [generic options] <input> <output>\n",
//					getClass().getSimpleName());
//			ToolRunner.printGenericCommandUsage(System.err);
//			return -1;
//		}
		
		Job job = Job.getInstance(getConf(), "Max temperature");
				
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
		System.exit(exitCode);
	}

	//测试使用一个正在运行的本地作业运行器
	@Test
	public void test() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		//指定执行方式为本地作业运行器
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 1);
		
		Path input = new Path("input");
		Path output = new Path("output");
		
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[] {
				input.toString(), output.toString()
		});
		
		assertThat(exitCode, is(0));
		checkOutput(conf, output);
	}
}
