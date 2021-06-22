#!/usr/bin/python3
"""runet stats calculation"""

import argparse
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, approx_count_distinct
from pyspark.sql.functions import col, when, window


N_PARTITIONS = 8


def setup_parser():
    """
    customizes default parser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-brokers", required=True)
    parser.add_argument("--topic-name", required=True)
    parser.add_argument("--starting-offsets", default="latest")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--processing-time", default="0 seconds")
    group.add_argument("--once", action="store_true")
    return parser


def calc_runet_stats():
    """calcs runet stats"""

    parser = setup_parser()
    args = parser.parse_args()
    if args.once:
        args.processing_time = None
    else:
        args.once = None

    sc = SparkContext()
    spark = SparkSession(sc)
    sc.setLogLevel("WARN")
    # spark.sparkContext.setLogLevel("WARN")

    spark.conf.set("spark.sql.shuffle.partitions", N_PARTITIONS)
    spark.conf.set("spark.sql.session.timeZone", "Europe/Moscow")

    input_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_brokers)
        .option("subscribe", args.topic_name)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    stringed_df = input_stream_df.selectExpr("cast(value as string)")
    list_df = stringed_df.selectExpr("SPLIT(value, '\t') as values")
    list_df.createOrReplaceTempView("list_df")
    uid_domain_df = spark.sql(
        "SELECT to_timestamp(from_unixtime(values[0])) as ts,\
            values[1] as uid, lower(parse_url(values[2], 'HOST')) as domain from list_df"
    )
    uid_domain_df = uid_domain_df.withColumn(
        "zone", when(col("domain").endswith(".ru"), "ru").otherwise("not ru")
    )

    window_df = (
        uid_domain_df.groupBy(window(col("ts"), "2 seconds", "1 second"), col("zone"))
        .agg(
            count(col("uid")).alias("view"),
            approx_count_distinct(col("uid")).alias("unique"),
        )
        .orderBy(
            col("window").asc(),
            col("view").desc(),
            col("zone").asc(),
        )
        .limit(20)
    )

    query_out = (
        window_df.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(once=args.once, processingTime=args.processing_time)
        .start()
    )

    query_out.awaitTermination()


calc_runet_stats()
