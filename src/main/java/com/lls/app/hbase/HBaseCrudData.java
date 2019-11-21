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
public class HBaseCrudData {

    private Table getTable(String host, String tableName) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        conf.set("hbase.zookeeper.property.clientPort", "12181");
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection.getTable(TableName.valueOf(tableName));
    }

    public void put(String zkUrl, String tableName) throws Exception {
        Table table = this.getTable(zkUrl, tableName);

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

    public void get(String zkUrl, String tableName) throws Exception {
        Table table = this.getTable(zkUrl, tableName);
        Get get = new Get(Bytes.toBytes("row1"));
        get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        Result result = table.get(get);
        byte[] values = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        System.out.println("Value: " + Bytes.toString(values));
    }

    public static void main(String[] args) throws Exception {
        HBaseCrudData crud = new HBaseCrudData();
        crud.put("localhost", "test_table_002");
    }

}
