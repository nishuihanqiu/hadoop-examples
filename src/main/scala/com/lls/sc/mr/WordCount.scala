package com.lls.sc.mr

import org.apache.spark.SparkContext

object WordCount {

    def main(args: Array[String]): Unit = {
        val inputPath = args(0)
        val outputPath = args(1)
        val sparkContext = new SparkContext()
        val lines = sparkContext.textFile(inputPath)
        val wordCounts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
        wordCounts.saveAsTextFile(outputPath)
    }

}
