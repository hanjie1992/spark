from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.setAppName('TestDStream')
conf.setMaster('local')
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 10)
ssc.sparkContext.setLogLevel("ERROR")
lines = ssc.textFileStream('../data/streaming')
words = lines.flatMap(lambda line: line.split(' '))
wordCounts = words.map(lambda x : (x,1)).reduceByKey(lambda a,b:a+b)
# DStream.pprint(num=10)
# .pprint()方法打印DStream中每个RDD的前几个元素，
# 元素数量通过参数num指定，默认为10。
wordCounts.pprint()
# streamingContext类的start()函数开始这个流式应用的运行
ssc.start()
"""
调用awaitTermination()，driver将阻塞在这里，直到流式应用意外退出。
另外，通过调用stop()函数可以优雅退出流式应用
"""
ssc.awaitTermination()
