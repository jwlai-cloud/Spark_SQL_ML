import re
from pyspark import SparkConf, SparkContext


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

book = sc.textFile("file:///Users/sdljw/PycharmProjects/Spark_SQL_ML/dataset/book.txt")
words = book.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    if cleanWord := word.encode('ascii', 'ignore'):
        print(f"{cleanWord.decode()} {str(count)}")
