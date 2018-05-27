package cn.itcast.mr.ii;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;



public class DPSYStepOne {
    public static class DPSYStepOneMpper extends Mapper<LongWritable, Text, Text, LongWritable>{
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, " ");
            String phone=fields[0];

            //获取这一行数据所在的文件切片
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            //从文件切片中获取文件名
            String fileName= inputSplit.getPath().getName();

            context.write(new Text(phone+"-->"+fileName), new LongWritable(1));
        }
    }

    public static class DPSYStepOneReduce extends Reducer<Text, LongWritable, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            String[] fields = StringUtils.split(key.toString(), "-->");
            String phone=fields[0];
            String fileName=fields[1];
            long numberCounter=0;

            for (LongWritable value : values) {
                numberCounter+=value.get();
            }

            context.write(new Text(phone), new Text(fileName+" "+numberCounter));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DPSYStepOneMpper.class);

        job.setMapperClass(DPSYStepOneMpper.class);
        job.setReducerClass(DPSYStepOneReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //检查一下参数所指定的输出路径是否存在，如果已存在，先删除
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output, true);
        }

        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
