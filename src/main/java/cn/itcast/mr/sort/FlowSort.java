package cn.itcast.mr.sort;

import cn.itcast.mr.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSort {
	public static class FlowSumMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = StringUtils.split(line, " ");
			String phone=words[0];
			long up_flow=Long.parseLong(words[1]);
			long d_flow=Long.parseLong(words[2]);
			
			context.write(new FlowBean(phone, up_flow, d_flow),NullWritable.get());
			
			
		}
	}
	
	public static class FlowSumReduce extends Reducer<FlowBean,NullWritable, Text, FlowBean> {
		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> value,
				Context context)
				throws IOException, InterruptedException {
			context.write(new Text(key.getPhone()), key);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf =new Configuration();
		Job jobs=Job.getInstance(conf);
		jobs.setJarByClass(FlowSort.class);
		
		/*Counters counters = jobs.getCounters();
		Counter input_records = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS);
		long total = input_records.getValue();*/
		
		jobs.setMapperClass(FlowSumMapper.class);
		jobs.setReducerClass(FlowSumReduce.class);
		
		jobs.setOutputKeyClass(Text.class);
		jobs.setOutputValueClass(FlowBean.class);
		
		jobs.setMapOutputKeyClass(FlowBean.class);
		jobs.setMapOutputValueClass(NullWritable.class);
		
		
		FileInputFormat.setInputPaths(jobs, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobs, new Path(args[1]));
		
//		FileInputFormat.setInputPaths(jobs, new Path("hdfs://itcast01:9000/flowfile/"));
//		FileOutputFormat.setOutputPath(jobs, new Path("hdfs://itcast01:9000/flow/output"));
		
	
		 System.exit(jobs.waitForCompletion(true)?0:1);
	}
}
