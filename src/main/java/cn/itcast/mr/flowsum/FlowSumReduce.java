package cn.itcast.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSumReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

	//���ÿ����һ������<1387788654,{flowbean,flowbean,flowbean,flowbean.....}>����һ�����ǵ�reduce����
	//reduce�е�ҵ���߼����Ǳ���values��Ȼ������ۼ���������
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values,
			Context context)
			throws IOException, InterruptedException {
		long up_flow_sum = 0;
		long d_flow_sum = 0;
		for (FlowBean value : values) {

			up_flow_sum += value.getUp_flow();
			d_flow_sum += value.getD_flow();

		}

		context.write(key, new FlowBean(key.toString(), up_flow_sum, d_flow_sum));
	}
}
