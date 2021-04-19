from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf=conf)


def extract_customer_pricepairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))


book = sc.textFile("file:///Users/sdljw/PycharmProjects/Spark_SQL_ML/dataset/customer-orders.csv")
mappedInput = book.map(extract_customer_pricepairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

results = totalByCustomer.collect();
for result in results:
    print(result)
