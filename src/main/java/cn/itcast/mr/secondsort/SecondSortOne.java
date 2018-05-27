package cn.itcast.mr.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class SecondSortOne extends Configured implements Tool{

    public static class SecondMapper extends Mapper<LongWritable, Text, IntPair, IntWritable>{
        private final IntPair intkey = new IntPair();
        private final IntWritable intvalue = new IntWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int left = 0;
            int right = 0;
            if (tokenizer.hasMoreTokens())
            {
                left = Integer.parseInt(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens())
                    right = Integer.parseInt(tokenizer.nextToken());
                intkey.set(left, right);
                intvalue.set(right);
                context.write(intkey, intvalue);
            }
        }
    }

    public static class SecondReduce extends Reducer<IntPair, IntWritable, Text, IntWritable>{
        private final Text left = new Text();
        private static final Text SEPARATOR = new Text("------------------------------------------------");

        public void reduce(IntPair key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
        {
            context.write(SEPARATOR, null);
            left.set(Integer.toString(key.getFirst()));
            for (IntWritable val : values)
            {
                context.write(left, val);
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, IntWritable>
    {
        @Override
        public int getPartition(IntPair key, IntWritable value,int numPartitions)
        {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    public static class GroupingComparator extends WritableComparator
    {
        protected GroupingComparator()
        {
            super(IntPair.class, true);
        }
        @Override
        //Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int l = ip1.getFirst();
            int r = ip2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // 读取hadoop配置
        Configuration conf = new Configuration();
        // 实例化一道作业
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondSortOne.class);
        // Mapper类型
        job.setMapperClass(SecondMapper.class);
        // 不再需要Combiner类型，因为Combiner的输出类型<Text, IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
        //job.setCombinerClass(Reduce.class);
        // Reducer类型
        job.setReducerClass(SecondReduce.class);
        // 分区函数
        job.setPartitionerClass(FirstPartitioner.class);
        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        // map 输出Key的类型
        job.setMapOutputKeyClass(IntPair.class);
        // map输出Value的类型
        job.setMapOutputValueClass(IntWritable.class);
        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // rduce输出Value的类型
        job.setOutputValueClass(IntWritable.class);

        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        //job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        //job.setOutputFormatClass(TextOutputFormat.class);

        arg0=new String[2];
//      arg0[0]="hdfs://itcast03:9000/secondsort/in";
//      arg0[1]="hdfs://itcast03:9000/secondsort/out";
//      arg0[0]="d:/secondsort/in";
//      arg0[1]="d:/secondsort/out5";
        arg0[0]="/secondsort/in";
        arg0[1]="/secondsort/out";

        // 输入hdfs路径
        FileInputFormat.setInputPaths(job, new Path(arg0[0]));
        // 输出hdfs路径
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
        // 提交job
        return job.waitForCompletion(true)? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new Configuration(),new SecondSortOne(),args);
        System.exit(res);
    }

}
