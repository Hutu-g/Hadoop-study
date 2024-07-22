package org.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * FileInputFormat 切片规则
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取job对象
        Configuration configuration = new Configuration();
        Job job =  Job.getInstance(configuration);
        //2.关联driver类
        job.setJarByClass(WCDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        //4.设置mapper输出k和v
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置程序输出k和v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //设置时候虚拟切片为4M，切片变为3片
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

        //6.设置输入和输出
        FileInputFormat.setInputPaths(job,new Path("E:\\bigdata_project\\Hadoop-study\\mapreduce\\input\\combinetextinputformat"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\bigdata_project\\Hadoop-study\\mapreduce\\output\\combinetextinputformat\\wc2"));
        //7.提交
        job.waitForCompletion(true);
    }
}
