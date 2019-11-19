package com.lls.app.mr.coordination;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/************************************
 * AverageRunner step5
 * 计算总和评分
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class AverageRunner implements Runner {

    @Override
    public boolean run(Configuration config, Map<String, String> map) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        String jobName = "averageRunner";
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(AverageRunner.class);
        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path inPath = new Path(map.get(Constants.AVERAGE_RUNNER_INPUT));
        Path outPath = new Path(map.get(Constants.AVERAGE_RUNNER_OUTPUT));
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true);
    }

    private static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //输入格式："u2732	i405,2.0"
            String[] values = Pattern.compile("[\t,]").split(value.toString());
            Text k = new Text(values[0]);						//key: userID
            Text v = new Text(values[1] + "," + values[2]);		//value: "itemID,评分"
            context.write(k, v);
        }
    }

    private static class AverageReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap<>();	//用于对商品评分累加
            for (Text val : values) {	//val格式: "itemID,评分"
                String[] items = val.toString().split(",");
                String itemID = items[0];
                Double score = Double.parseDouble(items[1]);

                if (map.containsKey(itemID)) {	//如果Map中已记录该商品，取出评分累加后重新写入Map
                    map.put(itemID, map.get(itemID) + score);
                } else {
                    map.put(itemID, score);
                }
            }

            //遍历Map，完成输出
            //根据itemID创建迭代器对象
            //取出itemID
            for (String itemID : map.keySet()) {
                double score = map.get(itemID);                    //根据itemID从map中取出score
                context.write(key, new Text(itemID + "," + score));    //格式："userid	itemID,score"
            }
        }
    }

}
