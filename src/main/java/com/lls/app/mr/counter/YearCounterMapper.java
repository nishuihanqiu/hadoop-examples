package com.lls.app.mr.counter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/************************************
 * YearCounterMapper
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class YearCounterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("[,\\t\\s]");
        String s = values[0].trim();
        Text date = new Text(s);                //获取日期
        context.write(date, one);               //将日期和常数1作为Map输出

        //根据KEY值不同，增加对应计数器的值
        if (s.startsWith("2015")) {
            context.getCounter(YearEnum.Y_2015).increment(1);
        } else if (s.startsWith("2016")) {
            context.getCounter(YearEnum.Y_2016).increment(1);
        } else {
            context.getCounter(YearEnum.Y_2017).increment(1);
        }

    }
}
