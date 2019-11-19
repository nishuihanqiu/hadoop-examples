package com.lls.app.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/************************************
 * DataSort
 * @author liliangshan
 * @date 2019/11/18
 ************************************/
public class DataSort {

    public void asc(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.设置HDFS配置信息
        Configuration conf = new Configuration();
        // 2.设置MapReduce作业配置信息
        String jobName = "DateSortAsc"; // 定义作业名称
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(DataSort.class); // 指定作业类

        job.setMapperClass(DataAscMapper.class); // 指定Mapper类
        job.setMapOutputKeyClass(IntWritable.class); // 设置Mapper输出Key类型
        job.setMapOutputValueClass(Text.class); // 设置Mapper输出Value类型

        job.setReducerClass(DataAscReducer.class); // 指定Reducer类
        job.setOutputKeyClass(Text.class); // 设置Reduce输出Key类型
        job.setOutputValueClass(IntWritable.class); // 设置Reduce输出Value类型

        // 指定排序所使用的比较器
        job.setSortComparatorClass(DataComparator.AscComparator.class);

        // 3.设置作业输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[2]);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        // 4.运行作业
        System.out.println("Job: " + jobName + " is running...");
        if (job.waitForCompletion(true)) {
            System.out.println("success!");
        } else {
            System.out.println("failed!");
        }
    }

    public void desc(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.设置HDFS配置信息
        Configuration conf = new Configuration();
        // 2.设置MapReduce作业配置信息
        String jobName = "DateSortDesc"; // 定义作业名称
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(DataSort.class); // 指定作业类

        job.setMapperClass(DataAscMapper.class); // 指定Mapper类
        job.setMapOutputKeyClass(IntWritable.class); // 设置Mapper输出Key类型
        job.setMapOutputValueClass(Text.class); // 设置Mapper输出Value类型

        job.setReducerClass(DataAscReducer.class); // 指定Reducer类
        job.setOutputKeyClass(Text.class); // 设置Reduce输出Key类型
        job.setOutputValueClass(IntWritable.class); // 设置Reduce输出Value类型

        // 指定排序所使用的比较器
        job.setSortComparatorClass(DataComparator.DescComparator.class);

        // 3.设置作业输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[2]);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        // 4.运行作业
        System.out.println("Job: " + jobName + " is running...");
        if (job.waitForCompletion(true)) {
            System.out.println("success!");
        } else {
            System.out.println("failed!");
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        DataSort dataSort = new DataSort();
        if (args.length < 3) {
            System.out.println("args must be gt 3");
            System.out.println("Usage [options]:a|d [input dirs] [output dirs]");
            System.exit(1);
        }

        if (args[0].equals("a")) {
            dataSort.asc(args);
        } else if (args[0].equals("d")) {
            dataSort.desc(args);
        } else {
            System.out.println("args[0] must be a|d");
        }

        System.exit(0);
    }


}
