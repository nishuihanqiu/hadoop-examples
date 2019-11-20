package com.lls.app.mr.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/************************************
 * CommonFriends
 * @author liliangshan
 * @date 2019/11/20
 ************************************/
public class CommonFriends {


    public void execute(String[] args) throws Exception {
        String hdfs = "hdfs://localhost:9000";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfs);

        // job1配置信息
        Job job1 = Job.getInstance(conf, "DecomposeJob");
        job1.setJarByClass(CommonFriends.class);
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
        }

        if (!job1.waitForCompletion(true)) {
            System.out.println("failed..................");
            System.exit(1);
            return;
        }

        // job1如果运行成功则进入job2, job2完全依赖job1的结果，所以job1成功执行就开启job2
        // job2配置信息
        Job job2 = Job.getInstance(conf, "MergeJob");
        job2.setJarByClass(CommonFriends.class);
        job2.setMapperClass(MergeFriendsMapper.class);
        job2.setReducerClass(MergeFriendsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        Path output2 = new Path(args[2]);
        FileInputFormat.addInputPath(job2, output1); // 输入是job1的输出
        FileOutputFormat.setOutputPath(job2, output2);
        if (fs.exists(output2)) {
            fs.delete(output2, true);
        }
        if (job2.waitForCompletion(true)) {
            System.out.println("success.................");
            System.exit(0);
        } else {
            System.out.println("failed..................");
            System.exit(1);
        }
    }


    public static void main(String[] args) throws Exception {
        CommonFriends commonFriends = new CommonFriends();
        commonFriends.execute(args);
    }

}
