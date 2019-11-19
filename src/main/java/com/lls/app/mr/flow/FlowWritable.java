package com.lls.app.mr.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/************************************
 * FlowWritable
 * @author liliangshan
 * @date 2019/11/18
 ************************************/
public class FlowWritable implements Writable {

    private int up;
    private int down;
    private int sum;

    public FlowWritable() {

    }

    public FlowWritable(int up, int down) {
        this.up = up;
        this.down = down;
        this.sum = this.up + this.down;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(up);
        out.writeInt(down);
        out.writeInt(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.up = in.readInt();
        this.down = in.readInt();
        this.sum = in.readInt();
    }

    @Override
    public String toString() {
        return "up=" + up + ", down=" + down + ", sum=" + sum;
    }

    public int getDown() {
        return down;
    }

    public void setDown(int down) {
        this.down = down;
    }

    public int getUp() {
        return up;
    }

    public void setUp(int up) {
        this.up = up;
    }

    public int getSum() {
        return sum;
    }

}
