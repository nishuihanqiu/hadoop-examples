package com.lls.app.hdfs;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.runtime.BoxedUnit;

/************************************
 * HdFsApplication
 * @author liliangshan
 * @date 2020/7/25
 ************************************/
public class HdFsApplication {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("spark://liliangshan.local:7077")
                .setAppName("HdFsApplication");
        SparkContext context = new SparkContext(conf);
        RDD<String> rdd = context.textFile("hdfs://liliangshan.local:9000/input/ali-test.kubeconfig", 2);
    }

}
