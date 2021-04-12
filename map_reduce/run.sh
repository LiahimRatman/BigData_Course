#!/bin/bash

set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar

hdfs dfs -rm -r $2

yarn jar $HADOOP_STREAMING_JAR \
  -files mapper.py,reducer.py \
  -mapper mapper.py \
  -reducer reducer.py \
  -numReduceTasks 2 \
  -input $1 \
  -output $2

hdfs dfs -cat $2/part-00000 | head -50
hdfs dfs -cat $2/part-00000 | head -50 > hw2_mr_data_ids.out
