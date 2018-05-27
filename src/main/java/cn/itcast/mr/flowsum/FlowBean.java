package cn.itcast.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{

	private String phone;
	private long up_flow;
	private long d_flow;
	private long s_flow;
	
	public FlowBean(){}
		
		
		
	public FlowBean(String phone, long up_flow, long d_flow) {
		this.phone = phone;
		this.up_flow = up_flow;
		this.d_flow = d_flow;
		this.s_flow = up_flow+d_flow;
	}




	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public long getUp_flow() {
		return up_flow;
	}
	public void setUp_flow(long up_flow) {
		this.up_flow = up_flow;
	}
	public long getD_flow() {
		return d_flow;
	}
	public void setD_flow(long d_flow) {
		this.d_flow = d_flow;
	}
	public long getS_flow() {
		return s_flow;
	}
	public void setS_flow(long s_flow) {
		this.s_flow = s_flow;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeLong(up_flow);
		out.writeLong(d_flow);
		out.writeLong(s_flow);
		
		
		
	}
	
	//��������з����г���������?
	//��������ж��������ֶ�ʱ����������л�ʱ��˳�򱣳�һ��
	@Override
	public void readFields(DataInput in) throws IOException {
		 phone=in.readUTF();
		 up_flow=in.readLong();
		 d_flow=in.readLong();
		 s_flow=in.readLong();
		
	}
	
	@Override
	public String toString() {
		return " "+up_flow+" "+d_flow+" "+s_flow ;
	}
	
	public int compareTo(FlowBean flow) {
		return s_flow>flow.getS_flow()?-1:1;
	}
}
