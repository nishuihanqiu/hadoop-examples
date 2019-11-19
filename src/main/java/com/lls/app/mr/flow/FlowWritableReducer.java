package com.lls.app.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/************************************
 * FlowWritableReducer
 * @author liliangshan
 * @date 2019/11/18
 ************************************/
public class FlowWritableReducer extends Reducer<Text, FlowWritable, Text, FlowWritable> {

    @Override
    protected void reduce(Text key, Iterable<FlowWritable> values, Context context) throws IOException, InterruptedException {
        int up = 0;
        int down = 0;

        for (FlowWritable value : values) {
            up += value.getUp();
            down += value.getDown();
        }
        System.out.println(key.toString() + ":" + up + "," + down);
        context.write(key, new FlowWritable(up, down));
    }

}
