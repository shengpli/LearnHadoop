package cn.itcast.mr.areapartition;

import cn.itcast.mr.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对流量原始日志进行流量统计，将不同省份的用户统计结果输出到不同文件(自定义分组：根据map输出key分组)
 * 需要自定义改造两个机制：
 * 1、改造分区的逻辑，自定义一个partitioner
 * 2、自定义reduer task的并发任务数
 *
 * @author duanhaitao@itcast.cn
 *
 */
public class FlowSumByArea {
    private static class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields=StringUtils.split(line, " ");
            String phone=fields[0];
            long up_flow=Long.parseLong(fields[2]);
            long d_flow=Long.parseLong(fields[3]);

            context.write(new Text(phone), new FlowBean(phone, up_flow, d_flow));
        }

    }


    private static class FlowSumReduce extends Reducer<Text, FlowBean, Text, FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values,
                              Reducer<Text, FlowBean, Text, FlowBean>.Context context)
                throws IOException, InterruptedException {
            long up_flow_sum=0;
            long d_flow_sum=0;
            for (FlowBean value : values) {
                up_flow_sum+=value.getUp_flow();
                d_flow_sum+=value.getD_flow();
            }

            context.write(key, new FlowBean(key.toString(), up_flow_sum, d_flow_sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job flowJob=Job.getInstance(conf);
        flowJob.setJarByClass(FlowSumByArea.class);

        flowJob.setMapperClass(FlowSumMapper.class);
        flowJob.setReducerClass(FlowSumReduce.class);

        flowJob.setOutputKeyClass(Text.class);
        flowJob.setOutputValueClass(FlowBean.class);

        //设置我们自定义的分组逻辑定义
        flowJob.setPartitionerClass(AreaPartitioner.class);

        //设置reduce的任务并发数，应该跟分组的数量保持一致
        flowJob.setNumReduceTasks(5);

        FileSystem fs = FileSystem.get(conf);
        Path flowoutput=new Path(args[1]);
        if(fs.exists(flowoutput)){
            fs.delete(flowoutput, true);
        }

        FileInputFormat.setInputPaths(flowJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(flowJob,new Path(args[1]));

        System.exit(flowJob.waitForCompletion(true)?0:1);
    }
}
