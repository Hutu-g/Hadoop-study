package org.partitions.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        String phone = words[1];
        int up = Integer.parseInt(words[words.length - 3]);
        int down = Integer.parseInt(words[words.length - 2]);

        FlowBean flowBean = new FlowBean();
        flowBean.setUp(up);
        flowBean.setDown(down);
        flowBean.setSum(down + up);
        context.write(new Text(phone),flowBean);
    }
}
