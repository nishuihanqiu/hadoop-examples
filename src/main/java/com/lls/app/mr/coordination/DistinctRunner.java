package com.lls.app.mr.coordination;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/************************************
 * DistinctRunner step1
 * distinct
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class DistinctRunner implements Runner {

    @Override
    public boolean run(Configuration config, Map<String, String> map) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        String jobName = "distinctRunner";
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(DistinctRunner.class);
        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        Path inPath = new Path(map.get(Constants.DISTINCT_RUNNER_INPUT));
        Path outPath = new Path(map.get(Constants.DISTINCT_RUNNER_OUTPUT));
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true);
    }


    private static class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() != 0) {    //过滤掉输入文件标题行
                context.write(new Text(value.toString().trim()), NullWritable.get());
            }
        }
    }

    private static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}
