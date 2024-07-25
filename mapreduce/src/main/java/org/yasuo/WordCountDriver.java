package org.yasuo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: hutu
 * @Date: 2024/7/15 16:37
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取job对象
        Configuration configuration = new Configuration();
        Job job =  Job.getInstance(configuration);

//        // 开启map端输出压缩
//        configuration.setBoolean("mapreduce.map.output.compress", true);
//        // 设置map端输出压缩方式
//        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        //2.关联driver类
        job.setJarByClass(WordCountDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        //4.设置mapper输出k和v
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置程序输出k和v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.设置输入和输出
        FileInputFormat.setInputPaths(job,new Path("E:\\bigdata_project\\Hadoop-study\\mapreduce\\input\\wcount\\words.txt"));

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        FileOutputFormat.setOutputPath(job,new Path("E:\\bigdata_project\\Hadoop-study\\mapreduce\\output\\yasuo\\wc02"));
        //7.提交
        job.waitForCompletion(true);
    }

}
