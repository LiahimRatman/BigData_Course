import re
from pyspark import SparkContext


sc = SparkContext()
wiki_rdd = sc.textFile("hdfs:///data/wiki/en_articles_part")
words_rdd = (
    wiki_rdd
    .map(lambda x: x.split('\t', 1))
    .map(lambda pair: pair[1].lower())
    .map(lambda content: ' '.join(content.split()))
    .flatMap(lambda text: re.findall(r"narodnaya \w+", text))
    .map(lambda x: ('_'.join(x.split()), 1))
    .reduceByKey(lambda x, y: x + y)
)
result1 = words_rdd.takeOrdered(1000, key=lambda x: x[0])
for item in result1:
    print('\t'.join(list(map(str, item))))
