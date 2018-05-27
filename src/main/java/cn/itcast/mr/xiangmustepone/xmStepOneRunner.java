package cn.itcast.mr.xiangmustepone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.itcast.mr.flowsum.FlowBean;


public class xmStepOneRunner extends Configured implements Tool{
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf=new Configuration();
		//conf.set("mapreduce.job.jar", "xmstepone.jar");
		Job job =Job.getInstance(conf);
		
		job.setJarByClass(xmStepOneRunner.class);
		
		job.setMapperClass(xmStepOneMapper.class);
		job.setReducerClass(xmStepOneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
//		FileInputFormat.setInputPaths(job, new Path("hdfs://ns1/home/xmstepone"));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://ns1/home/output4"));
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int res=ToolRunner.run(new Configuration(),new xmStepOneRunner(),args);
		System.exit(res);
	}
}
