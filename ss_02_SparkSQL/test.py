from pyspark.sql import SparkSession,Row
from pyspark import SparkContext,SparkConf

spark = SparkSession.builder\
    .config('spark.driver.extraClassPath', 'E:\software\mysql-connector-java-5.1.46.jar')\
    .getOrCreate()
jdbcDF = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://localhost:3306/spark")\
    .option("dbtable","student")\
    .option("user","root")\
    .option("password","123456")\
    .load()
print(jdbcDF.show())