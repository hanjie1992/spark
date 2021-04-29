from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()
peopleDF = spark.read.json("../data/sparkSql/people.json")
peopleDF.select("name", "age").write.format("json")\
    .save("../data/sparkSql/new_people.json")
peopleDF.select("name").write.format("text")\
    .save("../data/sparkSql/new_people.txt")

