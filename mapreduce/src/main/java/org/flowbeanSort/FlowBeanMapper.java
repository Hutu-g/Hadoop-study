package org.flowbeanSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanMapper extends Mapper<LongWritable, Text, FlowBean,Text> {
    private FlowBean flowBean = new FlowBean();
    private Text value = new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] splitLine = line.split("\t");

        flowBean.setUp(Integer.parseInt(splitLine[1]));
        flowBean.setDown(Integer.parseInt(splitLine[2]));
        flowBean.setSum(Integer.parseInt(splitLine[3]));
        value.set(splitLine[0]);
        context.write(flowBean,value);
    }
}
