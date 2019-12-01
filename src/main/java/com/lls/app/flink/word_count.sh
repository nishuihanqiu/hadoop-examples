#!/usr/bin/env bash

# 开启nc
nc -l 9000

# 再执行
~/flink-1.9.1/bin/flink run --class com.lls.app.flink.SocketWindowWordCount  hadoop-world-1.0-SNAPSHOT.jar --port 9000

# web submit
# jar: hadoop-world-1.0-SNAPSHOT.jar
# mian class：com.lls.app.flink.SocketWindowWordCount
# parameters: --port 9000