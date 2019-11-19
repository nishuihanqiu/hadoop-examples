package com.lls.app.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/************************************
 * DataAscMapper
 * @author liliangshan
 * @date 2019/11/18
 ************************************/
public class DataAscMapper extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable writable = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        writable.set(Integer.parseInt(values[1].trim()));
        // 将次数作为key进行升序排序
        context.write(writable, new Text(values[0].trim()));
        System.out.println(writable.get() + "," + values[0].trim());
    }

}
