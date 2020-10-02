package com.lls.app.mr.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * JoinMapper .
 *
 * @author liliangshan
 * @date 2020/9/30
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static final String LEFT_FILE_NAME = "student_info.csv";
    public static final String RIGHT_FILE_NAME = "student_class_info.csv";
    public static final String LEFT_FILE_FLAG = "l";
    public static final String RIGHT_FILE_FLAG = "r";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
        String fileFlag = "";
        String joinKey = "";
        String joinValue = "";
        String[] values = value.toString().split(",");
        if (filePath.contains(LEFT_FILE_NAME)) {
            fileFlag = LEFT_FILE_FLAG;
            joinKey = values[1].trim();
            joinValue = values[0].trim();
        } else if (filePath.contains(RIGHT_FILE_NAME)) {
            fileFlag = RIGHT_FILE_FLAG;
            joinKey = values[0].trim();
            joinValue = values[1].trim();
        }

        context.write(new Text(joinKey), new Text(joinValue + "," + fileFlag));
    }
}
