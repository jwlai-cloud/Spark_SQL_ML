from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# Create schema when reading customer-orders
customerOrderSchema = StructType([ \
                                  StructField("cust_id", IntegerType(), True),
                                  StructField("item_id", IntegerType(), True),  
                                  StructField("amount_spent", FloatType(), True)
                                  ])

# Load up the data into spark dataset
customersDF = spark.read.schema(customerOrderSchema).csv("file:///Users/sdljw/PycharmProjects/Spark_SQL_ML/dataset/customer-orders.csv")

# More sophiscated aggregation in GroupBy object will be in "agg" then using func.xxx
totalByCustomer = customersDF.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2) \
                                      .alias("total_spent"))

totalByCustomerSorted = totalByCustomer.sort("total_spent")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
