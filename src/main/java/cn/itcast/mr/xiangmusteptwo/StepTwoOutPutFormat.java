package cn.itcast.mr.xiangmusteptwo;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StepTwoOutPutFormat<K, V> extends FileOutputFormat<K, V>{

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataOutputStream fsout1 = fs.create(new Path("/home/xm/output1"));
		FSDataOutputStream fsout2 = fs.create(new Path("/home/xm/output2"));
		return new RecordWriterImpl(fsout1,fsout2);
	}
	
	public static class RecordWriterImpl<K, V> extends RecordWriter<K, V>{
		
		public FSDataOutputStream fsoutone=null;
		public FSDataOutputStream fsouttwo=null;

		public RecordWriterImpl(FSDataOutputStream fsout1,FSDataOutputStream fsout2){
			this.fsoutone=fsout1;
			this.fsouttwo=fsout2;
		}
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			if(fsoutone==null){
				fsoutone.close();
			}
			if(fsouttwo==null){
				fsouttwo.close();
			}
			
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			String line=key.toString();
			String[] words = StringUtils.split(line,"\t");
			  
			if(words[words.length-1]=="daipa"){
				fsouttwo.writeUTF(line);
				fsouttwo.writeUTF("\n");
			}else{
				fsoutone.writeUTF(line);
				fsouttwo.writeUTF("\n");
			}
		}
		
	}

}
