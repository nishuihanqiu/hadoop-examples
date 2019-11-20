package com.lls.app.mr.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/************************************
 * CommonFriendsControl
 * @author liliangshan
 * @date 2019/11/20
 ************************************/
public class CommonFriendsControl {

    public void execute(String[] args) throws Exception {
        String hdfs = "hdfs://localhost:9000";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfs);

        Job job1 = Job.getInstance(conf, "DecomposeJob");
        job1.setJarByClass(CommonFriendsControl.class);
        job1.setMapperClass(DecomposeFriendsMapper.class);
        job1.setReducerClass(DecomposeFriendsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        Path input = new Path(args[0]);
        Path output1 = new Path(args[1]);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, output1);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output1)) {
            fs.delete(output1, true);
            System.out.println("我被删了");// 打印可见只被删了一次，有点怪
        }
        // ControlledJob作业控制容器
        ControlledJob ctrJob1 = new ControlledJob(conf);
        ctrJob1.setJob(job1);// job1加入控制容器

        Job job2 = Job.getInstance(conf, "Merge");
        job2.setJarByClass(CommonFriendsControl.class);
        job2.setMapperClass(MergeFriendsMapper.class);
        job2.setReducerClass(MergeFriendsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        Path input2 = new Path(args[1]);
        Path output2 = new Path(args[2]);
        FileInputFormat.addInputPath(job2, input2);
        FileOutputFormat.setOutputPath(job2, output2);
        if (fs.exists(output2)) {
            fs.delete(output2, true);
        }
        ControlledJob ctrJob2 = new ControlledJob(conf);
        ctrJob2.setJob(job2);// job2加入作业控制容器

        // 添加作业依赖，表明job2依赖job1执行
        ctrJob2.addDependingJob(ctrJob1);

        // 定义作业主控制容器，监控、调度job1，job2
        JobControl jobControl = new JobControl("JobControl");
        jobControl.addJob(ctrJob1);
        jobControl.addJob(ctrJob2);
        // 启动作业线程
        Thread T = new Thread(jobControl);
        T.start();
        while (true) {
            if (jobControl.allFinished()) {// 等待作业全部结束
                System.out.println(jobControl.getSuccessfulJobList());// 打印成功job信息
                jobControl.stop();
                break;
            }
        }
        /* *
         * 打印控制信息如下
         * [job name:	Decompose
         job id:	JobControl0
         job state:	SUCCESS
         job mapred id:	job_local445604445_0001
         job message:	just initialized
         job has no depending job:
         , job name:	Merge
         job id:	JobControl1
         job state:	SUCCESS
         job mapred id:	job_local1897659504_0002
         job message:	just initialized
         job has 1 dependeng jobs:
         depending job 0:	Decompose
         ]
         */
    }

    public static void main(String[] args) throws Exception {
        CommonFriendsControl control = new CommonFriendsControl();
        control.execute(args);
    }

}
