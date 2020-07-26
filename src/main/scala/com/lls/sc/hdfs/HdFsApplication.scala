package com.lls.sc.hdfs

import org.apache.spark.{SparkConf, SparkContext}

object HdFsApplication {

//    读取hdfs文件数据
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("HdFsApplication")
        val sparkContext = new SparkContext(conf)
        val lines = sparkContext.textFile("hdfs://liliangshan.local:9000/input/ali-test.kubeconfig")
        lines.foreach(println(_))
        sparkContext.stop()
    }

}
