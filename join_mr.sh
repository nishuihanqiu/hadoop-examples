#gradle clean build

hdfs dfs -rm -r /input/student
hdfs dfs -mkdir /input/student

hdfs dfs -put ./src/main/java/com/lls/app/mr/join/student_info.csv /input/student/student_info.csv
hdfs dfs -put ./src/main/java/com/lls/app/mr/join/student_class_info.csv /input/student/student_class_info.csv

hdfs dfs -rm -r /output/student
hadoop jar build/libs/hadoop-world-1.0-SNAPSHOT.jar com.lls.app.mr.join.JoinDriver  /input/student /output/student

