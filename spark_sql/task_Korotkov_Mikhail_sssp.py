from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F


SRC, DST = 12, 34
PATH = "hdfs:///data/twitter/twitter.txt"
sc = SparkContext()
spark = SparkSession(sc)
graph = (
    spark.read.format("csv")
    .options(header=False, sep="\t", inferSchema=True)
    .load(PATH)
)
graph = graph.selectExpr("_c1 as src", "_c0 as dst")
run_df = graph.filter(f"src = {SRC}")
counter = 1
while True:
    left = run_df.selectExpr("dst").withColumnRenamed("dst", "src").alias("left")
    right = graph.withColumnRenamed("src", "src_cp").alias("right")
    run_df = left.join(right, F.expr(" left.src = right.src_cp"), "inner").selectExpr("dst")
    counter += 1
    found = run_df.filter(F.expr(f"dst = {DST}")).count()
    if found:
        break

print(counter)
