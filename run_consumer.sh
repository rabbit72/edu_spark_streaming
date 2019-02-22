#!/bin/sh

hdfs dfs -rm -r -f $1

spark-submit \
    --master "local[*]" \
    --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1,org.elasticsearch:elasticsearch-spark-20_2.11:6.6.1 \
    consumer.py $1 $2
