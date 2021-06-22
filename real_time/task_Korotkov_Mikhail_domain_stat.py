#!/usr/bin/python3
"""domain stats calculation"""

import argparse
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, approx_count_distinct


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


def calc_domain_stats():
    """calcs domain stats"""

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
        "SELECT values[1] as uid, parse_url(values[2], 'HOST') as domain from list_df"
    )
    # uid_domain_df.createOrReplaceTempView("uid_domain_df")
    # spark.sql("SELECT domain, count(domain) as view,
    # count(distinct uid) as unique from uid_domain_df group by domain order by view desc")

    domain_counts_df = (
        uid_domain_df.groupBy("domain")
        .agg(
            count("domain").alias("view"), approx_count_distinct("uid").alias("unique")
        )
        .orderBy("view", ascending=False)
        .limit(10)
    )

    query_out = (
        domain_counts_df.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(once=args.once, processingTime=args.processing_time)
        .start()
    )

    query_out.awaitTermination()


calc_domain_stats()
