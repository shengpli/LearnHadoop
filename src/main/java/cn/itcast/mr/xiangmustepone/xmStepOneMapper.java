package cn.itcast.mr.xiangmustepone;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.itcast.mr.flowsum.FlowBean;

public class xmStepOneMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = StringUtils.split(line, "\t");
		String url=null;
		long up_flow=0;
		long d_flow=0;
		
		if(words.length>4&&words[0]!=null){
			 url=words[0];
			 up_flow=Long.parseLong(words[3]);
			 d_flow=Long.parseLong(words[4]);
			 context.write(new Text(url),new FlowBean("", up_flow, d_flow));
		}
		
		
		
		
	}
}
