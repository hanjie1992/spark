

# 1.步骤1：导入pyspark模块
"""
由于程序中需要用到拆分字符串和展开数组内的所有
单词的功能，所以引用了来自pyspark.sql.functions
里面的split和explode函数
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import explode

# 2.步骤2：创建SparkSession对象
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# 3．步骤3：创建输入数据源
"""
创建一个输入数据源，从“监听在本机
（localhost）的9999端口上的服务”那里
接收文本数据，具体语句如下：
"""
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4.步骤4：定义流计算过程
# 有了输入数据源以后，接着需要定义相关的
# 查询语句，具体如下：
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

wordCounts = words.groupBy("word").count()

# 5.步骤5：启动流计算并输出结果
# 定义完查询语句后，下面就可以开始真正执行
#  流计算，具体语句如下：
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="8 seconds") \
    .start()
print(wordCounts)
query.awaitTermination()
print(wordCounts)