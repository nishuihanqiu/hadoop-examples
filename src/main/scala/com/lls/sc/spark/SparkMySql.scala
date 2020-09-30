package com.lls.sc.spark

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSONObject

object SparkMySql {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SparkMySql")
        val sc = new SparkContext(conf)
        val inputMysql = new JdbcRDD[(Int, String, String, String, String)](sc,
            () => {
                Class.forName("com.mysql.jdbc.Driver")
                DriverManager.getConnection("jdbc:mysql://liliangshan.local:3306/test?useUnicode=true&characterEncoding=utf8",
                    "root", "admin")
            },
            "SELECT `id`, `phone`, `name`, `gender`, `avatar` FROM active_users WHERE id >=? AND id <= ?", 800000, 800179, 1,
            r => (r.getInt(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5))
        )
        println("查询到的记录条目数：" + inputMysql.count())
        inputMysql.foreach(println)
        val mapList = inputMysql.map(r => JSONObject(
            Map("id" -> r._1, "phone" -> r._2, "name" -> r._3, "gender" -> r._4, "avatar" -> r._5)))
        val path = "/Users/liliangshan/Desktop/duola/hadoop-world/active_users.json"
        mapList.saveAsTextFile(path)
        sc.stop()
    }

}
