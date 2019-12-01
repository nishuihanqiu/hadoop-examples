package com.lls.app.mr.avro;

import com.lls.app.avro.User;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/************************************
 * ColorCountMapper
 * @author liliangshan
 * @date 2019/11/21
 ************************************/
public class ColorCountMapper extends Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {

    @Override
    protected void map(AvroKey<User> key, NullWritable value, Context context) throws IOException, InterruptedException {
        CharSequence color = key.datum().getFavoriteColor();
        if (color == null) {
            color = "none";
        }
        context.write(new Text(color.toString()), new IntWritable(1));
    }
}
