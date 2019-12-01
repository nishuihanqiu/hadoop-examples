package com.lls.app.mr.avro;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/************************************
 * ColorCountReducer
 * @author liliangshan
 * @date 2019/11/21
 ************************************/
public class ColorCountReducer extends Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(new AvroKey<>(key.toString()), new AvroValue<>(sum));
    }
}
