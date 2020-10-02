package com.lls.app.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * JoinDriver .
 *
 * @author liliangshan
 * @date 2020/9/30
 */
public class JoinDriver {

    // hdfs dfs -put ./src/main/java/com/lls/app/mr/join/student_info.csv /input/student_info.csv
    // hdfs dfs -put ./src/main/java/com/lls/app/mr/join/student_class_info.csv /input/student_class_info.csv
    // hadoop jar build/libs/hadoop-world-1.0-SNAPSHOT.jar com.lls.app.mr.join.JoinDriver  /input /output


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "JoinMR");
        job.setJarByClass(JoinDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
