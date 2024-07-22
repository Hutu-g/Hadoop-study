package org.writable.writablecomparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 排序
 */
public class FlowBeanDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job =  Job.getInstance(configuration);
        //2.关联driver类
        job.setJarByClass(FlowBeanDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);
        //4.设置mapper输出k和v
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //5.设置程序输出k和v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置输入和输出
        FileInputFormat.setInputPaths(job,new Path("E:\\bigdata_project\\Hadoop-study\\mapreduce\\output\\flowbeansort\\wc01"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\bigdata_project\\Hadoop-study\\mapreduce\\output\\flowbeansort\\wc02"));
        //7.提交
        job.waitForCompletion(true);
    }
}
