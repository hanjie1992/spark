from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()
peopleDF = spark.read.json("../data/sparkSql/people.json")
#打印Schema信息,以树形结构输出
print(peopleDF.printSchema())
# select() 指定输出列
print(peopleDF.select(peopleDF["name"],peopleDF["age"]+1).show())
# filter() 条件过滤
print(peopleDF.filter(peopleDF["age"]>20).show())
# groupBy() 分组聚合
print(peopleDF.groupby("age").count().show())
# sort() 排序
print(peopleDF.sort(peopleDF["age"].desc()).show())
print(peopleDF.sort(peopleDF["age"].desc(),peopleDF["name"].asc()).show())