#!/usr/bin/env bash
set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${2}*
hdfs dfs -rm -r -skipTrash ${2}_tmp

# Wordcount
( yarn jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name="Streaming HabrCount. Phase 1" \
    -D stream.num.map.output.key.fields=2 \
    -D stream.num.reduce.output.key.fields=2 \
    -files mapper_hw3.py,reducer_hw3.py \
    -mapper mapper_hw3.py \
    -combiner reducer_hw3.py \
    -reducer reducer_hw3.py \
    -numReduceTasks 8 \
    -input $1 \
    -output ${2}_tmp &&

# useful flags
# complex key
    # -D mapreduce.map.output.key.field.separator=. \
    # -D stream.num.map.output.key.fields=2 \
    # -combiner cat \
    # -D stream.num.reduce.output.key.fields=2 \
# partitioner
    # -D mapreduce.partition.keypartitioner.options=-k2.1,2.1 \
    # -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
# comparator
    # -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    # -D mapreduce.partition.keycomparator.options="-k1,1nr -k2,2" \
# bad practice
    # -D stream.map.output.field.separator=. \

# Global sorting as we use only 1 reducer
yarn jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name="Streaming HabrCount. Phase 2" \
    -D stream.num.map.output.key.fields=3 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,1n -k2,2nr" \
    -files mapper2_hw3.py,reducer2_hw3.py \
    -mapper mapper2_hw3.py \
    -reducer reducer2_hw3.py \
    -numReduceTasks 1 \
    -input ${2}_tmp \
    -output ${2}
)

hdfs dfs -cat ${2}/* | head -20
