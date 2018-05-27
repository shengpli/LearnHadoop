package cn.itcast.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class FlowSumRunner extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf =new Configuration();
		//conf.set("mapreduce.job.jar", "flow.jar");
		
		
		
		Job jobs=Job.getInstance(conf);
		jobs.setJarByClass(FlowSumRunner.class);
		
		
		jobs.setMapperClass(FlowSumMapper.class);
		jobs.setReducerClass(FlowSumReduce.class);
		
		jobs.setOutputKeyClass(Text.class);
		jobs.setOutputValueClass(FlowBean.class);
		
		jobs.setMapOutputKeyClass(Text.class);
		jobs.setMapOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(jobs, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobs, new Path(args[1]));
		
//		FileInputFormat.setInputPaths(jobs, new Path("hdfs://itcast01:9000/flowfile/"));
//		FileOutputFormat.setOutputPath(jobs, new Path("hdfs://itcast01:9000/flow/output"));
		
	
		return jobs.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int res =ToolRunner.run(new Configuration(), new FlowSumRunner(), args);
		System.exit(res);
	}
}
