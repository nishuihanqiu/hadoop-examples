#!/usr/bin/env bash

## 基于物品的协同过滤算法实现

# clean
hdfs dfs -rm -r /input/coordination/
hdfs dfs -rm -r /output/coordination/

# step1
hdfs dfs -mkdir -p /input/coordination/distinct
hdfs dfs -mkdir -p /output/coordination/distinct

# step2
hdfs dfs -mkdir -p /output/coordination/score

# step3
hdfs dfs -mkdir -p /output/coordination/viewer

# step4
hdfs dfs -mkdir -p /output/coordination/single_score

# step5
hdfs dfs -mkdir -p /output/coordination/average

# step6
hdfs dfs -mkdir -p /output/coordination/top

# put data
hdfs dfs -put ./coordination.csv /input/coordination/distinct

# change to project directory
cd ~/Desktop/duola/hadoop-world/ || exit 1

gradle clean

gradle build -xtest

hadoop jar ./build/libs/hadoop-world-1.0-SNAPSHOT.jar com.lls.app.mr.coordination.Coordination






