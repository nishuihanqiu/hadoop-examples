package com.lls.app.mr.coordination;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/************************************
 * ViewerRunner step3
 * 计算用户同显矩阵
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class ViewerRunner implements Runner {

    private final static Text KEY = new Text();
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    public boolean run(Configuration config, Map<String, String> map) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        String jobName = "step3";
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(ViewerRunner.class);
        job.setMapperClass(ViewerMapper.class);
        job.setReducerClass(ViewerReducer.class);
        job.setCombinerClass(ViewerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path inPath = new Path(map.get(Constants.VIEWER_RUNNER_INPUT));
        Path outPath = new Path(map.get(Constants.VIEWER_RUNNER_OUTPUT));
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true);
    }

    private static class ViewerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // u2727 i468:2,i446:3
            String[] items = value.toString().split("\t")[1].split(","); //每件商品和评分列表，格式：i468:2 i446:3
            for (String item : items) {
                String itemA = item.split(":")[0]; // itemA = i468 .. i446
                for (String s : items) {
                    String itemB = s.split(":")[0]; // itemB = i468 .. i446
                    KEY.set(itemA + ":" + itemB);     // i468:i468 , i468:i446, i446:i468, i446:i446
                    context.write(KEY, ONE);
                }
            }
        }
    }

    private static class ViewerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
