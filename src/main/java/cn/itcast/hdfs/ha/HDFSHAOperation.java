package cn.itcast.hdfs.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HDFSHAOperation {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs =FileSystem.get(new URI("hdfs://ns1/"), conf, "root");
		fs.copyFromLocalFile(new Path("d:/qingshu.txt"), new Path("hdfs://ns1/"));
	}
}
