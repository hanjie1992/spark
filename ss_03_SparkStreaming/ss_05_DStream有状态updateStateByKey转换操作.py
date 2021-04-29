
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.setAppName('TestDStream')
conf.setMaster('local[2]')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)
ssc.checkpoint("data")
# RDD with initial state (key, value) pairs
initialStateRDD = sc.parallelize([(u'hello', 1), (u'world', 1)])
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)
lines = ssc.socketTextStream("localhost", 9999)
running_counts = lines.flatMap(lambda line: line.split(" "))\
                      .map(lambda word: (word, 1))\
                      .updateStateByKey(updateFunc, initialRDD=initialStateRDD)
running_counts.pprint()
ssc.start()
ssc.awaitTermination()