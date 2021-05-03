from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("file:///Users/sdljw/PycharmProjects/Spark_SQL_ML/dataset/book.txt")

# Split using a regular expression that extracts words
# Unstructure data, read in text file, the column is "value"
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalize everything to lowercase
# When you want to operate on certain columns, use "select" first
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results. - All the data
wordCountsSorted.show(wordCountsSorted.count())

spark.stop()