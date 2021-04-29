from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()
# sparkDF = spark.read.text("../data/sparkSql/people.txt")
sparkDF = spark.read.json("../data/sparkSql/people.json")
print(sparkDF)
print(sparkDF.show())