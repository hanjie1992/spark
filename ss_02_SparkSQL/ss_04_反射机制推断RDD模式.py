from pyspark import SparkConf
from pyspark.sql import SparkSession,Row

spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()

peopleRDD = spark.sparkContext.textFile("../data/sparkSql/people.txt")\
    .map(lambda line: line.split(","))\
    .map(lambda p: Row(name=p[0], age=int(p[1])))

schemaPeople = spark.createDataFrame(peopleRDD)
#必须注册为临时表才能供下面的查询使用
schemaPeople.createOrReplaceTempView("people")
# personsDF = spark.sql("select name,age from people where age > 20").show()
personsDF = spark.sql("select name,age from people where age > 20")
print(personsDF.show())
#DataFrame中的每个元素都是一行记录，包含name和age两个字段，分别用p.name和p.age来获取值
personsRDD=personsDF.rdd.map(lambda p:"Name: "+p.name+ ","+"Age: "+str(p.age))
personsRDD.foreach(print)




