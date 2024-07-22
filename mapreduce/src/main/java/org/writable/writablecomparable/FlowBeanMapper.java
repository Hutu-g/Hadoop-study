package org.writable.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanMapper extends Mapper<LongWritable, Text, FlowBean,Text> {
    private FlowBean outK = new FlowBean();
    private Text outV = new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] splitLine = line.split("\t");

        outK.setUp(Integer.parseInt(splitLine[1]));
        outK.setDown(Integer.parseInt(splitLine[2]));
        outK.setSum(Integer.parseInt(splitLine[3]));
        outV.set(splitLine[0]);
        context.write(outK,outV);
    }
}
