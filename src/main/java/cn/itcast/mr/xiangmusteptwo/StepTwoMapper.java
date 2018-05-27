package cn.itcast.mr.xiangmusteptwo;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
//读入原始数据
//抽取其中的url查询规则库得到众多的内容识别信息 网站类别，频道类别，主题词，关键词，影片名，主演，导演。。。。
//将分析结果追加到原始日志后面
//context.write( 时间戳 ..... destip srcip ... url .. . get 200 ...
//网站类别，频道类别，主题词，关键词，影片名，主演，导演。。。。)
//如果某条url在规则库中查不到结果，则输出到带爬清单
//context.write( url tocrawl)
public class StepTwoMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

    private HashMap<String, String> ruleMap = new HashMap<>();

    // setup方法是在mapper task 初始化时被调用一次
    protected void setup(Context context) throws IOException ,InterruptedException {
        DBLoader.dbLoader(ruleMap);
    }
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(line, '\t');
        String url=words[0];
        String result;
        String info=ruleMap.get(url);
        if(info==null){
            result=url+"\t"+"daipa";
            context.write(new Text(result), NullWritable.get());
        }else{
            result=line+"\t"+info;
            context.write(new Text(result), NullWritable.get());
        }

    }
}
