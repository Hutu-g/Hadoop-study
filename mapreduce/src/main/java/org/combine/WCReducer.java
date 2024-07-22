package org.combine;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //重写Reduce方法
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            //循坏将相同字符的个数集合相加
            sum += value.get();
        }
        //写出
        context.write(key, new IntWritable(sum));
    }
}
