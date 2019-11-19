package com.lls.app.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/************************************
 * DataComparator
 * @author liliangshan
 * @date 2019/11/18
 ************************************/
public class DataComparator extends WritableComparator {

    public DataComparator() {
        super(IntWritable.class, true);
    }

    @SuppressWarnings({"rawtypes", "unchecked"}) // 不检查类型
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // CompareTo方法，返回值为1则降序，-1则升序
        // 默认是a.compareTo(b)，a比b小返回-1，现在反过来返回1，就变成了降序
        return a.compareTo(b);
    }

    public static class AscComparator extends DataComparator {

        public AscComparator() {
            super();
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return super.compare(a, b);
        }
    }

    public static class DescComparator extends DataComparator {

        public DescComparator() {
            super();
        }

        @SuppressWarnings({"rawtypes", "unchecked"}) // 不检查类型
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return b.compareTo(a);
        }
    }
}
