package com.lls.app.mr.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * JoinReducer .
 *
 * @author liliangshan
 * @date 2020/9/30
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    public static final String LEFT_FILE_NAME = "student_info.csv";
    public static final String RIGHT_FILE_NAME = "student_class_info.csv";
    public static final String LEFT_FILE_FLAG = "l";
    public static final String RIGHT_FILE_FLAG = "r";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        List<String> studentClassNames = new ArrayList<>();
        String studentName = "";

        while (iterator.hasNext()) {
            String[] infos = iterator.next().toString().split(",");
            if (infos[1].trim().equals(LEFT_FILE_FLAG)) {
                studentName = infos[0].trim();
            } else if (infos[1].trim().equals(RIGHT_FILE_FLAG)) {
                studentClassNames.add(infos[0].trim());
            }
        }

        for (String studentClassName : studentClassNames) {
            context.write(new Text(studentName), new Text(studentClassName));
        }
    }
}
