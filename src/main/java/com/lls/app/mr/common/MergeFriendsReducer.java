package com.lls.app.mr.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/************************************
 * MergeFriendsReducer
 * @author liliangshan
 * @date 2019/11/20
 ************************************/
public class MergeFriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder friend = new StringBuilder();
        for (Text value : values) {
            friend.append(value.toString()).append(",");
        }
        System.out.println(key.toString() + " " + friend);
        context.write(key, new Text(friend.toString()));
    }
}
