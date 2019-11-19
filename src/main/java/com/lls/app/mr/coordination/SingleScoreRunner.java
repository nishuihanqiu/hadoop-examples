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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/************************************
 * SingleScoreRunner step4
 * 同显矩阵*评分矩阵，计算评分单项
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class SingleScoreRunner implements Runner {


    @Override
    public boolean run(Configuration config, Map<String, String> map) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        String jobName = "singleScoreRunner";
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(SingleScoreRunner.class);
        job.setMapperClass(SingleScoreMapper.class);
        job.setReducerClass(SingleScoreReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path[] inPaths = new Path[]{
                new Path(map.get(Constants.SINGLE_SCORE_RUNNER_INPUT_I)),
                new Path(map.get(Constants.SINGLE_SCORE_RUNNER_INPUT_II))
        };
        Path outPath = new Path(map.get(Constants.SINGLE_SCORE_RUNNER_OUTPUT));
        FileInputFormat.setInputPaths(job, inPaths);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true);
    }

    private static class SingleScoreMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();    //根据上下文获取输入分片对象
            flag = split.getPath().getParent().getName();            //获取输入分片所属的目录名称
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = Pattern.compile("[\t,]").split(value.toString());
            if (flag.equals("output3")) {                //输入的是同现矩阵，values 格式："i100:i105 1"
                String[] items = values[0].split(":");
                String itemID1 = items[0];                //第一个商品id  "i100"
                String itemID2 = items[1];                //第二个商品id	 "i105"
                String num = values[1];                    //两件商品的同现次数    "1"

                Text k = new Text(itemID1);
                Text v = new Text("A:" + itemID2 + "," + num);    //格式："A:i105,1"
                context.write(k, v);                            //格式："i100	A:i105,1"

            } else if (flag.equals("output2")) {    //输入的是评分矩阵，values 格式："u14 i100:1 i25:1"
                String userID = values[0];
                for (int i = 1; i < values.length; i++) {
                    String[] vector = values[i].split(":");    //i100:1
                    String itemID = vector[0];
                    String score = vector[1];
                    Text k = new Text(itemID);
                    Text v = new Text("B:" + userID + "," + score);    //格式："B:u14,1"
                    context.write(k, v);                            //格式："i100 B:u14,1" 和 "i25 B:u14,1"
                }
            }
        }
    }

    private static class SingleScoreReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> mapA = new HashMap<>();
            Map<String, Integer> mapB = new HashMap<>();
            //reduce输入格式："i100  A:i105,1  A:i107,2  B:u14,1  B:u22,3"
            for (Text val : values) {    //将AB格式的输入分别放入HashMap中
                String str = val.toString();
                if (str.startsWith("A:")) {            //str格式："A:i105,1"
                    String[] kv = Pattern.compile("[\t,]").split(str.substring(2));
                    mapA.put(kv[0], Integer.parseInt(kv[1]));
                } else if (str.startsWith("B:")) {    //str格式："B:u14,1"
                    String[] kv = Pattern.compile("[\t,]").split(str.substring(2));
                    mapB.put(kv[0], Integer.parseInt(kv[1]));
                }
            }

            double result = 0;
            //根据mapA中key键(itemID)生成迭代器对象
            //获得itemID
            for (String k : mapA.keySet()) {
                int num = mapA.get(k);                //根据itemID从mapA获取同现次数
                //根据mapB中key键生成迭代器对象
                //userID
                for (String s : mapB.keySet()) {
                    int score = mapB.get(s);            //根据userID从mapB中获取用户行为评分
                    result = num * score;                            //矩阵相乘，计算评分
                    context.write(new Text(s), new Text(k + "," + result));    //输出 key："userID" value:"itemID,result"
                }
            }
        }
    }
}
