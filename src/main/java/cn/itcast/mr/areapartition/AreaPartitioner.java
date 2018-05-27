package cn.itcast.mr.areapartition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class AreaPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE>{
	private static HashMap<String,Integer> areaMap=new HashMap<String, Integer>();
	
	static{
		areaMap.put("132", 0);
		areaMap.put("133", 1);
		areaMap.put("134", 2);
		areaMap.put("135", 3);
		
	}
	@Override
	public int getPartition(KEY key, VALUE value, int areanum) {
		areanum=areaMap.get(key.toString().substring(0, 3))!=null?(areaMap.get(key.toString().substring(0, 3))):4;
		return areanum;
	}
	
}
