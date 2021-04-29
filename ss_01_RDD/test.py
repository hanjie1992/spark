
from pyspark import SparkContext, SparkConf

conf = SparkConf(). \
    setMaster("local"). \
    setAppName("spark")
# 创建SparkContext，该对象是提交Spark App的入口
sc = SparkContext(conf=conf)
sc.pa