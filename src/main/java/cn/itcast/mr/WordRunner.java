package cn.itcast.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//使用实现Tool接口，通过ToolRunner启动应用方便设置和获取Configuration配置
public class WordRunner extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //增加资源文件,被定义final的属性不会被后面添加的配置覆盖
        //conf.addResource("conf/core-site.xml");
        //conf.addResource("conf/hdfs-site.xml");


        //启动map中间输出压缩
        //conf.setBoolean("mapred.compress.map.output", true);
        //conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        Job jobs = Job.getInstance(conf);
        //设置了JarByClass则不需要明确指定jar的名称，hadoop会利用这个类来查找包含他的jar文件
        //1.需设置HADOOP_CLASSPATH路径，路径包含jar
        //2.使用hadoop cn.itcast.mr.WCRunner inputPath outputPath 即可提交
        //如果没设置JarByClass，使用hadoop jar wc.jar cn.itcast.mr.WCRunner inputPath outputPath提交
        jobs.setJarByClass(WordRunner.class);

        //本job使用的mapper和reducer的类
        jobs.setMapperClass(WordCountMapper.class);
        jobs.setReducerClass(WordCountReduce.class);

        //指定reduce的输出数据kv类型(如果reduce和map输入输出类型一致也会设置map类型)
        jobs.setOutputKeyClass(LongWritable.class);
        jobs.setOutputValueClass(Text.class);

        //指定map的输出数据kv类型
        jobs.setMapOutputKeyClass(Text.class);
        jobs.setMapOutputValueClass(LongWritable.class);

        //设置输出为压缩文件的方法。如果输入文件为压缩文件，mapreduce会自动解压，不需要任何设置，前提是支持这种压缩算法，hadoop checknative查看
        //FileOutputFormat.setCompressOutput(jobs, true);//启动输出压缩
        //FileOutputFormat.setOutputCompressorClass(jobs, GzipCodec.class);//指定压缩算法为Gzip

        //检查一下参数所指定的输出路径是否存在，如果已存在，先删除
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        //指定要处理的输入数据存放路径（可以单个文件、目录、文件匹配，还可以多次调用实现多路径输入）
        FileInputFormat.setInputPaths(jobs, new Path(args[0]));
        //指定要处理的输出数据存放路径(只能有一个)
        FileOutputFormat.setOutputPath(jobs, output);

        //将job提交给集群运行 ,方法中的true是个详细标识，作业会把进度写到控制台
        return jobs.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordRunner(), args);
        System.exit(res);
    }
}
