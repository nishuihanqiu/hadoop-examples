package com.lls.app.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/************************************
 * HBasePut
 * @author liliangshan
 * @date 2019/11/16
 ************************************/
public class HBasePutData {

    public void execute(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost:2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test_table_002"));

        Put put = new Put(Bytes.toBytes("row1"));  // row_key
        // 添加列数据，指定列簇、列名与列值
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("xiao ming"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("address"), Bytes.toBytes("shanghai"));

        Put put2 = new Put(Bytes.toBytes("row2"));  // row_key
        // 添加列数据，指定列簇、列名与列值
        put2.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("xiao ming 2"));
        put2.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes("22"));
        put2.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("address"), Bytes.toBytes("beijing"));

        Put put3 = new Put(Bytes.toBytes("row3"));  // row_key
        // 添加列数据，指定列簇、列名与列值
        put3.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes("25"));
        put3.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("address"), Bytes.toBytes("suzhou"));

        table.put(put);
        table.put(put2);
        table.put(put3);

        // 释放资源
        table.close();
        System.out.println("put data success!!!");

    }

    public static void main(String[] args) throws Exception {
        HBasePutData putData = new HBasePutData();
        putData.execute(args);
    }

}
