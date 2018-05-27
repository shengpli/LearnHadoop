package cn.itcast.mr.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair2 implements WritableComparable<IntPair2>{
	public int id;
	public String url;
	public int num;
	
	public IntPair2(){
		
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id=in.readInt();
		url=in.readUTF();
		num=in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(url);
		out.writeInt(num);
	}

	@Override
	public int compareTo(IntPair2 i) {
		 if (id != i.getId())
         {
             return id < i.getId() ? -1 : 1;
         }
         else if (num != i.getNum())
         {
             return num < i.getNum() ? -1 : 1;
         }
         else
         {
             return 0;
         }
		
	}

}
