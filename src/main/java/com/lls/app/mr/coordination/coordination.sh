#!/usr/bin/env bash

## 基于物品的协同过滤算法实现

# clean
hdfs dfs -rm -r /input/coordination
hdfs dfs -rm -r /output/coordination

# mkdir
hdfs dfs -mkdir -p /input/coordination
hdfs dfs -mkdir -p /output/coordination

# put data
hdfs dfs -put ./coordination.csv /input/coordination/data

# change to project directory
cd ~/Desktop/duola/hadoop-world/ || exit 1

gradle clean

gradle build -xtest

hadoop jar ./build/libs/hadoop-world-1.0-SNAPSHOT.jar com.lls.app.mr.coordination.Coordination






