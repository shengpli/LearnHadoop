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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class DPSYStepTwo {
    public static class DPSYStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line," " );
            String phone=fields[0];
            String fileName=fields[1];
            String numberCount=fields[2];

            context.write(new Text(phone),new Text(fileName+" "+numberCount));

        }
    }

    public static class DPSYStepTwoReduce extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String fields=null;
            for (Text value : values) {
                fields=fields+" "+value.toString();
            }
            context.write(key, new Text(fields));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DPSYStepTwo.class);

        job.setMapperClass(DPSYStepTwoMapper.class);
        job.setReducerClass(DPSYStepTwoReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

	/*	job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);*/

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
