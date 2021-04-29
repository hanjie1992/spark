from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .config('spark.driver.extraClassPath', 'E:\software\mysql-connector-java-5.1.46.jar')\
    .getOrCreate()

#下面设置模式信息
schema = StructType([StructField("id", IntegerType(), True), \
StructField("name", StringType(), True), \
StructField("gender", StringType(), True), \
StructField("age", IntegerType(), True)])

# 下面设置两条数据，表示两个学生的信息
studentRDD = spark \
    .sparkContext \
    .parallelize(["1000 Wang M 1129", "1001 Zhao M 1129"]) \
    .map(lambda x: x.split(" "))

# 下面创建Row对象，每个Row对象都是rowRDD中的一行
rowRDD = studentRDD.map(lambda p: Row(int(p[0].strip()), p[1].strip(), p[2].strip(), int(p[3].strip())))
# 建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
studentDF = spark.createDataFrame(rowRDD, schema)

# 写入数据库
prop = {}
prop['user'] = 'root'
prop['password'] = '123456'
prop['driver'] = "com.mysql.jdbc.Driver"
studentDF.write.jdbc("jdbc:mysql://localhost:3306/spark", 'student', 'append', prop)
