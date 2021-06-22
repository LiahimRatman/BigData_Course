#!/usr/bin/python3
"""movies table creation"""

import sys
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, split, trim, length, regexp_extract

sc = SparkContext()
spark = SparkSession(sc)
sc.setLogLevel("WARN")

PATH_TO_MOVIES_DATA = "/data/movielens/movies.csv"
GENRE_PLACEHOLDER = '(no genres listed)'
TABLE_NAME = 'movies'
KEYSPACE = sys.argv[1]

movies_rdd = spark.read.option("header", "true").csv(PATH_TO_MOVIES_DATA)

movies_rdd_correct = (
    movies_rdd
    .filter(col("genres") != GENRE_PLACEHOLDER)
    .select(
        col("movieId").alias("movieid"),
        split(col("genres"), '\|').alias("genres"),
        trim(col("title")).alias("title"),
        regexp_extract("title", "[\s]?\(([\d]{1,})\)(['\"\)]|\s?\([\sA-z]{0,}\))?[\s]?$", 1).alias('year'),
    )
    .filter(length(col("year")) == 4)
)

movies_cassandra = movies_rdd_correct.selectExpr("movieid", "title", "int(year) as year", "genres")
(
    movies_cassandra.write
    .format("org.apache.spark.sql.cassandra")
    .mode('append')
    .options(table=TABLE_NAME, keyspace=KEYSPACE)
    .save()
)