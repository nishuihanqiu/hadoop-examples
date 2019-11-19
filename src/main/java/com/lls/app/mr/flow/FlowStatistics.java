package com.lls.app.mr.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/************************************
 * FlowStatistics
 * @author liliangshan
 * @date 2019/11/18
 ************************************/
public class FlowStatistics {

    public void execute(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置hdfs配置信息
        String nameNodeIp = "localhost";
        String host = "hdfs://"+nameNodeIp+":9000";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", host);

        // 设置作业Job配置信息
        String jobName = "FlowStatistics";
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(FlowStatistics.class);
        // Map
		job.setMapperClass(FlowWritableMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowWritable.class);
        // Reduce
		job.setReducerClass(FlowWritableReducer.class);
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowWritable.class);

        // 设置job输入出路径
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        // 运行作业
        System.out.println("Job: " + jobName + " is running...");
        if(job.waitForCompletion(true)) {
            System.out.println("success!");
            System.exit(0);
        } else {
            System.out.println("failed!");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        FlowStatistics statistics = new FlowStatistics();
        statistics.execute(args);
    }

}
