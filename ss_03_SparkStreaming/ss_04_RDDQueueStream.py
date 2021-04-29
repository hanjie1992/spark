import time
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.setAppName('TestDStream')
conf.setMaster('local[2]')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)

#创建一个队列，通过该队列可以把RDD推给一个RDD队列流
rddQueue = []
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]
    time.sleep(1)
#创建一个RDD队列流
inputStream = ssc.queueStream(rddQueue)
mappedStream = inputStream.map(lambda x: (x % 10, 1))
reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
reducedStream.pprint()
ssc.start()
# 优雅的关闭流，等所有数据接收完再关闭
ssc.stop(stopSparkContext=True, stopGraceFully=True)
