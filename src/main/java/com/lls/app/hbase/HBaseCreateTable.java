package com.lls.app.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

/************************************
 * HBaseCreateTable
 * @author liliangshan
 * @date 2019/11/16
 ************************************/
public class HBaseCreateTable {

    public void execute(String[] args) throws Exception {
        // 创建HBase配置对象
        Configuration configuration = HBaseConfiguration.create();
        // 指定zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum", "localhost:2181");
        // 创建连接对象connection
        Connection connection = ConnectionFactory.createConnection(configuration);

        // 得到数据库管理员对象
        Admin admin = connection.getAdmin();
        // 创建表描述，并指定表名
        TableName tableName = TableName.valueOf("test_table_002");

        // 表描述
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        // 列簇
        ColumnFamilyDescriptorBuilder descriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("f1".getBytes());
        builder.setColumnFamily(descriptorBuilder.build());

        // 创建表
        admin.createTable(builder.build());

        System.out.println("create table success!!!");
    }

    public static void main(String[] args) throws Exception {
        HBaseCreateTable table = new HBaseCreateTable();
        table.execute(args);
    }

}
