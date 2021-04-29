from __future__ import print_function
import sys
import pymysql
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="PythonStreamingStatefulNetworkWordCount")
ssc = StreamingContext(sc, 1)
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

def dbfunc(records):
    db = pymysql.connect("localhost","root","123456","spark")
    cursor = db.cursor()
    def doinsert(p):
        sql = "insert into wordcount(word,count) values ('%s', '%s')" % (str(p[0]), str(p[1]))
        try:
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
    for item in records:
        doinsert(item)
def func(rdd):
    repartitionedRDD = rdd.repartition(3)
    repartitionedRDD.foreachPartition(dbfunc)
running_counts.foreachRDD(func)
ssc.start()
ssc.awaitTermination()
