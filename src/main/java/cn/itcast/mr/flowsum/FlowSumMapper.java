package cn.itcast.mr.flowsum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//�õ���־�е�һ�����ݣ��зָ����ֶΣ���ȡ��������Ҫ���ֶΣ��ֻ��ţ���������������������Ȼ���װ��kv���ͳ�ȥ
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] words=StringUtils.split(line, " ");
		
		String phone = words[0];
		long up_flow=Long.parseLong(words[2]);
		long d_flow=Long.parseLong(words[3]);
		String s_flow=words[2]+words[3];
		
		context.write(new Text(phone), new FlowBean(phone, up_flow, d_flow));
		
		
		
	}
}
