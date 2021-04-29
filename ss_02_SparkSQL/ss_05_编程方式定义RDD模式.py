from pyspark.sql.types import *
from pyspark.sql import SparkSession,Row
from pyspark import SparkConf

spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()

#下面生成“表头”
schemaString = "name age"
# 一个StructField就像一个SQL中的一个字段一样，它包含了這个字段的具体信息
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
# 一个StructType对象，可以有多个StructField,同时也可以用名字（name）来提取,
# 就想当于Map可以用key来提取value，但是他StructType提取的是整条字段的信息
schema = StructType(fields)

#下面生成“表中的记录”
lines = spark.sparkContext.textFile("../data/sparkSql/people.txt")
parts = lines.map(lambda x: x.split(","))
people = parts.map(lambda p: Row(p[0], p[1].strip()))

#下面把“表头”和“表中的记录”拼装在一起
schemaPeople = spark.createDataFrame(people, schema)
#注册一个临时表供下面查询使用
schemaPeople.createOrReplaceTempView("people")
results = spark.sql("SELECT name,age FROM people where age>20")
print(results.show())
