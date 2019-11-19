package com.lls.app.mr.coordination;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

/************************************
 * TopRunner step6
 * 评分排序取Top10
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class TopRunner implements Runner {

    private final static Text KEY = new Text();
    private final static Text VALUE = new Text();

    @Override
    public boolean run(Configuration config, Map<String, String> map) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        String jobName = "topRunner";
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(TopRunner.class);
        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setGroupingComparatorClass(UserGroupComparator.class);    //自定义分组

        Path inPath = new Path(map.get(Constants.TOP_RUNNER_INPUT));
        Path outPath = new Path(map.get(Constants.TOP_RUNNER_OUTPUT));
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true);
    }

    private static class TopMapper extends Mapper<LongWritable, Text, PairWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = Pattern.compile("[\t,]").split(value.toString());    //输入格式："u13	i524,3.0"
            String user = values[0];
            String item = values[1];
            String score = values[2];

            PairWritable k = new PairWritable();    //将uid和score封装到PairWritable对象中，作为MapKey输出
            k.setUid(user);
            k.setScore(Double.parseDouble(score));

            VALUE.set(item + ":" + score);    //将item和score组合，作为MapValue输出
            context.write(k, VALUE);        //输出格式：key:"u13 3.0"  value:"i524:3.0"
        }
    }

    private static class TopReducer extends Reducer<PairWritable, Text, Text, Text> {

        @Override
        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            StringBuilder sb = new StringBuilder();
            for (Text v : values) {
                if (i == 10) break;
                sb.append(v.toString()).append(",");    //将评分数前10项串联
                i++;
            }
            KEY.set(key.getUid());    //获取自定义key中的uid
            VALUE.set(sb.toString().substring(0, sb.toString().length() - 1));    //去掉最后的逗号
            context.write(KEY, VALUE);
        }
    }

    private static class PairWritable implements WritableComparable<PairWritable> {
        private String uid;
        private double score;

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        @Override
        public int compareTo(PairWritable o) {
            int r = this.uid.compareTo(o.getUid());    //按uid升序排列
            if (r == 0) {
                return -Double.compare(this.score, o.getScore()); //uid相同，则按score降序排列
            }
            return r;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(uid);
            out.writeDouble(score);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.uid = in.readUTF();
            this.score = in.readDouble();
        }

    }

    private static class UserGroupComparator extends WritableComparator {

        public UserGroupComparator() {
            super(PairWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable o1 = (PairWritable) a;
            PairWritable o2 = (PairWritable) b;
            return o1.getUid().compareTo(o2.getUid());
        }
    }

}
