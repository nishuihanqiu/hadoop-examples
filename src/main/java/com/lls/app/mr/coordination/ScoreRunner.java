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

/************************************
 * ScoreRunner step2
 * 计算用户评分矩阵
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class ScoreRunner implements Runner {

    @Override
    public boolean run(Configuration config, Map<String, String> map) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        String jobName = "ComputeRunner";
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(ScoreRunner.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path inPath = new Path(map.get(Constants.SCORE_RUNNER_INPUT));
        Path outPath = new Path(map.get(Constants.SCORE_RUNNER_OUTPUT));
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true);
    }

    private static class ScoreMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            String item = values[0].trim();        //商品id
            String user = values[1].trim();        //用户id
            String action = values[2].trim();      //用户行为
            Integer rv = Constants.ACTIONS.get(action);    //获取行为评分
            Text v = new Text(item + ":" + rv);    //value格式: "i1:1"
            Text k = new Text(user);
            context.write(k, v);            //map输出格式: "u2723  i1:1"
        }
    }

    private static class ScoreReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> m = new HashMap<>();    //用于存放每种商品的行为评分之和
            for (Text value : values) {
                String[] array = value.toString().split(":");
                String item = array[0];                        //商品id
                int score = Integer.parseInt(array[1]);    //行为评分
                score += m.get(item) == null ? 0 : m.get(item);    //计算用户对每件商品的行为评分和（如果Map集合中已有该商品评分，则累加）
                m.put(item, score);        //向HashMap中存入商品及评分之和
            }

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : m.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");    //将商品和评分串联，格式：  i1:1,i2:1,...I:N,
            }
            context.write(key, new Text(sb.toString().substring(0, sb.toString().length() - 1)));    //去掉最后的逗号

        }
    }
}
