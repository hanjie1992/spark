from pyspark import SparkContext

sc = SparkContext("local")
counter = 0

def increment(x):
    global counter
    counter += x

rdd = sc.parallelize(range(10))
rdd.foreach(increment)
print("Spark Counter value: ", counter)
