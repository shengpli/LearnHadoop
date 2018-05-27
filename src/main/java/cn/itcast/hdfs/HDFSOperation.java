package cn.itcast.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;


public class HDFSOperation {
	private static FileSystem fs=null;
	
	@Before
	public void init() throws Exception{
		//读取classpath下xxx-site.xml的配置信息
		Configuration conf=new Configuration();
		//也可以手动设置配置信息
		conf.set("fs.defaultFS", "hdfs://itcast01:9000/");
		//根据配置信息，去获取一个具体文件系统的客户端操作实例对象
		fs=FileSystem.get(new URI("hdfs://itcast01:9000/"), conf, "root");
	}
	/**
	 * 底层的方法
	 */
	//@Test	
	public void upload(){
		Configuration conf=new Configuration();
		try {
			FileSystem fs=FileSystem.get(conf);
			conf.set("fs.defaultFS", "hdfs://itcast01:9000/"); 
			Path pa=new Path("hdfs://itcast01:9000/aa/qingshu.txt");
			FSDataOutputStream fos=fs.create(pa);
			FileInputStream  fis=new FileInputStream("d:/qingshu.txt");
			IOUtils.copy(fis, fos);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//@Test
	/**
	 * 封装的方法
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void upload2() throws IllegalArgumentException, IOException{
		fs.copyFromLocalFile(new Path("d:/qingshu.txt"), new Path("hdfs://itcast01:9000/aaa/bbb/ccc/qingshu2.txt"));
		
	}
	
	@Test
	public void download()  {
		try {
			fs.copyToLocalFile(new Path("hdfs://itcast01:9000/aa/qingshu.txt"), new Path("d:/qingshu.txt"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//@Test
	public void selectFiles() throws FileNotFoundException, IllegalArgumentException, Exception{
		//获取所有文件信息（不含文件夹信息）
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus fileinfo = listFiles.next();
			Path path = fileinfo.getPath();
			String name = path.getName();
			System.out.println(name);
		}
		
		System.out.println("----------------------------");
		
		//listStatus 可以列出查询目录下文件和文件夹信息，但不提供自带的递归遍历
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus files:listStatus){
			String name = files.getPath().getName();
			System.out.println(name+(files.isDirectory()?" is dir":"is file"));
		}
		
	}
	//@Test
	public void mkdir() throws Exception, IOException{
		fs.mkdirs(new Path("/aaa/bbb/ccc"));
	}
	//@Test
	public void rmdir() throws IllegalArgumentException, IOException{
		fs.delete(new Path("/aaa"), true);//ture表示递归删除
	}
	
	public static void main(String[] args) throws Exception {			
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://itcast01:9000/");
		
		FileSystem fs=FileSystem.get(conf);
		Path pa=new Path("/hadoop-2.2.0.tar.gz");
		FSDataInputStream fos=fs.open(pa);
		FileOutputStream  fis=new FileOutputStream("d:/hadoop-2.2.0.tar.gz");
		IOUtils.copy(fos, fis);
		
	
	
	}
}
