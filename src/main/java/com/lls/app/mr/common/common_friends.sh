#!/usr/bin/env bash

## 计算出用户间的共同好友

# clean
hdfs dfs -rm -r /input/common_friends
hdfs dfs -rm -r /output/common_friends

# mkdir
hdfs dfs -mkdir -p /input/common_friends
hdfs dfs -mkdir -p /output/common_friends

# put data
hdfs dfs -put ./friends_data.txt /input/common_friends/data

# change to project directory
cd ~/Desktop/duola/hadoop-world/ || exit 1

gradle clean

gradle build -xtest

hadoop jar ./build/libs/hadoop-world-1.0-SNAPSHOT.jar com.lls.app.mr.common.CommonFriends /input/common_friends/data \
/output/common_friends/decompose_friends /output/common_friends/merge_friends

hdfs dfs -cat /output/common_friends/decompose_friends/p* || exit 1

hdfs dfs -cat /output/common_friends/merge_friends/p* || exit 1

hadoop jar ./build/libs/hadoop-world-1.0-SNAPSHOT.jar com.lls.app.mr.common.CommonFriendsControl /input/common_friends/data \
/output/common_friends/control_decompose_friends /output/common_friends/control_merge_friends

hdfs dfs -cat /output/common_friends/control_decompose_friends/p* || exit 1

hdfs dfs -cat /output/common_friends/control_merge_friends/p* || exit 1





