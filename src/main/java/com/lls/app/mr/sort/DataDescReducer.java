package com.lls.app.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/************************************
 * DataDescReducer
 * @author liliangshan
 * @date 2019/11/18
 ************************************/
public class DataDescReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            // 排序后再次颠倒k-v，将日期作为key
            System.out.println(value.toString() + ":" + key.get());
            context.write(value, key);
        }
    }
}
