package com.lls.app.mr.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/************************************
 * MergeFriendsMapper
 * @author liliangshan
 * @date 2019/11/20
 ************************************/
public class MergeFriendsMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Text user = new Text(value.toString().substring(0, 1));
        String[] friends = value.toString().substring(2).split(",");
        Arrays.sort(friends);// 要排好序，不然如A-B，B-A不能归并到一起
        //对如A B,C,E遍历输出如<B-C A>
        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                String pair = friends[i].trim() + "-" + friends[j].trim();
                context.write(new Text(pair), user);
            }
        }
    }
}
