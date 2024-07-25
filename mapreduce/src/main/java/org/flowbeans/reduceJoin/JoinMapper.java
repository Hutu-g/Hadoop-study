package org.flowbeans.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text,Text, JoinBean> {
    private String fileName;
    private JoinBean joinBean = new JoinBean();

    private Text outKey = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context) throws IOException, InterruptedException {
        //获取切片的文件的名字
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitLine = line.split("\t");
        if ("order.txt".equals(fileName)){
            String id = splitLine[0];
            String pid = splitLine[1];
            int amount = Integer.parseInt(splitLine[2]);
            outKey.set(pid);

            joinBean.setId(id);
            joinBean.setPid(pid);
            joinBean.setAmount(amount);
            joinBean.setPname("");
            joinBean.setFlag("order");
        }else {
            String pid = splitLine[0];
            String pname = splitLine[1];
            outKey.set(pid);

            joinBean.setId(" ");
            joinBean.setPid(pid);
            joinBean.setPname(pname);
            joinBean.setAmount(0);
            joinBean.setFlag("pd");
        }
        context.write(outKey,joinBean);
    }
}
