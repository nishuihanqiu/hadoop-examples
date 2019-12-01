#!/usr/bin/env bash

## 启动运行时报错，一定要检查一下Scala和spark的版本是否一致
$SPARK_HOME/bin/spark-submit --class "com.lls.sc.mr.WordCount" --master local[*] \
           ~/Desktop/duola/hadoop-world/build/libs/hadoop-world-1.0-SNAPSHOT.jar \
           ./inputs ./outputs