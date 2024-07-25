package org.yasuo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hutu
 * @Date: 2024/7/15 16:37
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text keyOut = new Text();
    IntWritable valueOut = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取到的value是Text类型，先进行转换
        String line = value.toString();
        //字符串处理，进行单词切分
        String[] words = line.split(" ");
        //循环设置mapper的输出k和v
        for (String word : words) {
            keyOut.set(word);
            valueOut.set(1);
            //写出
            context.write(keyOut, valueOut);
        }
    }
}
