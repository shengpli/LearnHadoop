package cn.itcast.mr.xiangmustepone;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.itcast.mr.flowsum.FlowBean;

public class xmStepOneReducer extends Reducer<Text, FlowBean, Text, LongWritable>{
	private TreeMap<FlowBean,String> treeMap=new TreeMap<FlowBean,String>();
	private  long flowsum=0;
	@Override
	//[key,value,values,...]
	//��treemapʵ������
	protected void reduce(Text key, Iterable<FlowBean> values,Context context)
			throws IOException, InterruptedException {
		long up_sum=0;
		long d_sum=0;
		for (FlowBean bean : values) {
			up_sum+=bean.getUp_flow();
			d_sum+=bean.getD_flow();
		}
		
		flowsum+=up_sum+d_sum;
		
		treeMap.put(new FlowBean("", up_sum, d_sum),key.toString());
			
	}
	
	@Override
	protected void cleanup(
			Context context)
			throws IOException, InterruptedException {
		Set<Entry<FlowBean,String>> entrySet = treeMap.entrySet();
		long s_flow_sum=0;
		for (Entry<FlowBean,String> entry : entrySet) {
			s_flow_sum+=entry.getKey().getS_flow();
			if(s_flow_sum/flowsum<=0.8){
				context.write(new Text(entry.getValue()), new LongWritable(entry.getKey().getS_flow()));
			}else{
				return;
			}
				
		}
	}
	
	
}
