#!/usr/bin/env bash

## 启动运行时报错，一定要检查一下Scala和spark的版本是否一致
#$SPARK_HOME/bin/spark-submit --class "com.lls.sc.mr.WordCount" --master local[*] \
#           ~/Desktop/duola/hadoop-world/build/libs/hadoop-world-1.0-SNAPSHOT.jar \
#           ./inputs ./outputs



# 独立集群上运行
#--master spark://liliangshan.local:7077

$SPARK_HOME/bin/spark-submit --class "com.lls.sc.mr.WordCount" --master spark://liliangshan.local:7077 \
           ~/Desktop/duola/hadoop-world/build/libs/hadoop-world-1.0-SNAPSHOT.jar \
           ./inputs ./outputs

# 这里执行报错可能是其他组件的版本不一致引起的，单独demo是可以的