from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

book = sc.textFile("file:///Users/sdljw/PycharmProjects/Spark_SQL_ML/dataset/book.txt")
words = book.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    if cleanWord := word.encode('ascii', 'ignore'):
        print(f"{cleanWord.decode()} {str(count)}")
