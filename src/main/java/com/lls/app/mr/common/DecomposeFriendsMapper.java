package com.lls.app.mr.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/************************************
 * DecomposeFriendsMapper
 * @author liliangshan
 * @date 2019/11/20
 ************************************/
public class DecomposeFriendsMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String values = value.toString();
        Text user = new Text(values.substring(0, 1));
        String[] friends = values.substring(2).split(",");

        //A:B,C,D,F,E,O
        for (int i = 0; i < friends.length; i++) {
            // 以<B，A>,<C,A>形式输出
            context.write(new Text(friends[i]), user);
        }
    }
}
