package com.lls.sc.pksql

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlBookMySql {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SparkSqlBookMySql")
        val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")

        val sqlContext = new SQLContext(sc)
        val ip = "127.0.0.1"
        val port = 3306
        var database = "test"
        var userName = "root"
        var password = "admin"
        val url = "jdbc:mysql://" + ip + ":" + port + "/" + database

        var activeUsersTable = "active_users"
        val dataFrameReader = sqlContext.read.format("jdbc").option("url", url)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", userName)
                .option("password", password)

        dataFrameReader.option("dbtable", activeUsersTable)
        val activeUserDataFrame = dataFrameReader.load()
        val activeUserStrutted = this.setActiveUserStructType(sqlContext, activeUserDataFrame)
        activeUserStrutted.show()
    }

    def setActiveUserStructType(sqlContext: SQLContext, dataFrame: DataFrame): DataFrame = {
//        val rdd = dataFrame.map(x =>
//            Row(x.getInt(0), x.getString(1), x.getString(2), x.getString(3), x.getInt(4)))
//        sqlContext.createDataFrame(rdd)

        return null
    }
}
