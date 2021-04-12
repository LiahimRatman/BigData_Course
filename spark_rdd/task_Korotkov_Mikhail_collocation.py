import re
import math as mt
from pyspark import SparkContext


sc = SparkContext()
wiki_rdd = sc.textFile("hdfs:///data/wiki/en_articles_part")
stopwords_rdd = sc.textFile("hdfs:///data/stop_words/stop_words_en-xpo6.txt")
stop_words_broadcast = sc.broadcast(stopwords_rdd.collect())
words_count = (
    wiki_rdd
    .map(lambda x: x.split('\t', 1))
    .map(lambda pair: pair[1].lower())
    .map(lambda text: re.findall(r"\w+", text))
    .map(lambda content: ([x for x in content if x not in stop_words_broadcast.value]))
    .map(lambda content: ' '.join(content))
    .flatMap(lambda text: re.findall(r"\w+", text))
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
)
wc_sum = (
    words_count
    .map(lambda x: (1, x[1]))
    .reduceByKey(lambda x, y: x + y)
)
sum_of_words = wc_sum.first()[1]
words_pairs_count = (
    wiki_rdd
    .map(lambda x: x.split('\t', 1))
    .map(lambda pair: pair[1].lower())
    .map(lambda text: re.findall(r"\w+", text))
    .map(lambda content: ([x for x in content if x not in stop_words_broadcast.value]))
    .flatMap(lambda x: [x[_] + ' ' + x[_ + 1] for _ in range(len(x) - 1)])
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
)
wp_sum = (
    words_pairs_count
    .map(lambda x: (1, x[1]))
    .reduceByKey(lambda x, y: x + y)
)
words_pairs_count_1 = (
    words_pairs_count
    .map(lambda x: (x[0].split()[0], [x[0].split()[1], x[1]]))
)
join1 = words_pairs_count_1.join(words_count)
join1_ = (
    join1
    .filter(lambda x: x[1][0][1] >= 500)
)
words_pairs_count_2 = (
    join1_
    .map(lambda x: (x[1][0][0], [x[0], x[1][0][1], x[1][1]]))
)
join2 = words_pairs_count_2.join(words_count)
words_pairs_count_fin = (
    join2
    .map(lambda x: (x[1][0][0] + '_' + x[0], round(
                    mt.log((x[1][0][1] / sum_of_words) /
                           (x[1][0][2] * x[1][1] / sum_of_words / sum_of_words)) /
                    ( - mt.log(x[1][0][1] / sum_of_words)), 3)))
)
results = words_pairs_count_fin.takeOrdered(39, key=lambda x: -x[1])
for item in results:
    print('\t'.join(list(map(str, item))))
