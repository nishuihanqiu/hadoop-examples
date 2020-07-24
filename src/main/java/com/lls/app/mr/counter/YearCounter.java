package com.lls.app.mr.counter;

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
 * YearCounter
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class YearCounter {
//    hdfs dfs -put ./src/main/java/com/lls/app/mr/counter/year.csv /input/year
//    hadoop jar build/libs/hadoop-world-1.0-SNAPSHOT.jar com.lls.app.mr.counter.YearCounter  /input/year /output

    public void execute(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置hdfs配置信息
        String nameNodeIp = "localhost";
        String host = "hdfs://"+nameNodeIp+":9000";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", host);

        //2.设置MapReduce作业配置信息
        String jobName = "YearCounter";						//作业名称
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(YearCounter.class);				//指定运行时作业类
        job.setMapperClass(YearCounterMapper.class);		//指定Mapper类
        job.setMapOutputKeyClass(Text.class);				//设置Mapper输出Key类型
        job.setMapOutputValueClass(IntWritable.class);		//设置Mapper输出Value类型
        job.setReducerClass(YearCounterReducer.class);		//指定Reducer类
        job.setOutputKeyClass(Text.class);					//设置Reduce输出Key类型
        job.setOutputValueClass(IntWritable.class); 		//设置Reduce输出Value类型

        //3.设置作业输入和输出路径
        String dataDir = args[0];			//实验数据目录
        String outputDir = args[1];		    //实验输出目录
        Path inPath = new Path(dataDir);
        Path outPath = new Path(outputDir);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        //4.运行作业
        System.out.println("Job: " + jobName + " is running...");
        if(job.waitForCompletion(true)) {
            System.out.println("success!");
            System.exit(0);
        } else {
            System.out.println("failed!");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        YearCounter counter = new YearCounter();
        counter.execute(args);
    }
}
