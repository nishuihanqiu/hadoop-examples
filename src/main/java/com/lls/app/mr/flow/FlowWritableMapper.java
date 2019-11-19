package com.lls.app.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/************************************
 * FlowWritableMapper
 * @author liliangshan
 * @date 2019/11/18
 ************************************/
public class FlowWritableMapper extends Mapper<Object, Text, Text, FlowWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        Text phone = new Text(values[0].trim());
        FlowWritable writable = new FlowWritable(Integer.parseInt(values[1].trim()), Integer.parseInt(values[2].trim()));
        System.out.println("Flow is " + writable.toString());
        context.write(phone, writable);
    }
}
