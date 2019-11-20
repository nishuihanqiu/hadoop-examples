package com.lls.app.mr.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/************************************
 * DecomposeFriendsReducer
 * @author liliangshan
 * @date 2019/11/20
 ************************************/
public class DecomposeFriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder friends = new StringBuilder();
        for (Text value : values) {
            friends.append(value.toString()).append(",");
        }
        // 输出个人所有好友，A	I,K,C,B,G,F,H,O,D
        context.write(key, new Text(friends.substring(0, friends.length() - 1)));
    }

}
