#!/usr/bin/env python3 
from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="PythonStreamingWindowedNetworkWordCount")
ssc = StreamingContext(sc, 10)
ssc.checkpoint("data_window")
lines = ssc.socketTextStream("localhost", 9999)
counts = lines.flatMap(lambda line: line.split(" "))\
              .map(lambda word: (word, 1))\
              . reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
counts.pprint()
ssc.start()
ssc.awaitTermination()
