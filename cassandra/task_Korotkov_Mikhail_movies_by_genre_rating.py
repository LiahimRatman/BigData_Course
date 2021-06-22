#!/usr/bin/python3
"""movies by genres table creation"""

import sys
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, split, trim, length, regexp_extract, explode, avg

sc = SparkContext()
spark = SparkSession(sc)
sc.setLogLevel("WARN")

PATH_TO_RATINGS_DATA = '/data/movielens/ratings.csv'
PATH_TO_MOVIES_DATA = "/data/movielens/movies.csv"
GENRE_PLACEHOLDER = '(no genres listed)'
TABLE_NAME = 'movies_by_genre_rating'
KEYSPACE = sys.argv[1]

ratings_rdd = spark.read.option("header", "true").csv(PATH_TO_RATINGS_DATA)
ratings_rdd = ratings_rdd.selectExpr("movieId as movieid", "float(rating) as rating")
ratings_rdd = (
    ratings_rdd
    .groupBy("movieid")
    .agg(
        avg("rating").alias("rating"),    
    )
)
ratings_rdd = ratings_rdd.selectExpr("movieId as movieid", "float(rating) as rating")
ratings_rdd.persist()

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
movies_rdd_correct.persist()

movies_with_ratings = movies_rdd_correct.join(ratings_rdd, "movieid", 'inner')
movies_with_ratings.persist()
movies_with_ratings_exploded_genres = (
    movies_with_ratings
    .select(
        col("movieid"),
        col("year"),
        explode(col("genres")).alias("genre"),
        col("title"),
        col("rating"),
    )
)
movies_with_ratings_exploded_genres = (
    movies_with_ratings_exploded_genres
    .selectExpr("movieid", "int(year) as year", "genre", "title", "rating")
)

(
    movies_with_ratings_exploded_genres.write
    .format("org.apache.spark.sql.cassandra")
    .mode('append')
    .options(table=TABLE_NAME, keyspace=KEYSPACE)
    .save()
)