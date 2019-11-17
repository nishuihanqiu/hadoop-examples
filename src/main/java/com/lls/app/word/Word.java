package com.lls.app.word;

import com.lls.app.Application;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/************************************
 * Word
 * @author liliangshan
 * @date 2019/11/15
 ************************************/
public class Word {

    public void execute(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word_count");

        // 指定本次job运行的主类
        job.setJarByClass(Application.class);

        // 指定本次job的具体mapper reducer实现类
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(WordReducer.class);
        job.setReducerClass(WordReducer.class);

        // 指定本次job reduce阶段的输出数据类型 也就是整个mr任务的最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定本次job待处理数据的目录 和程序执行完输出结果存放的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交本次job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

}
