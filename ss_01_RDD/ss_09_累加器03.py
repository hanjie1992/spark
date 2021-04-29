from pyspark import SparkContext

sc = SparkContext("local")
counter = sc.accumulator(0)
rdd = sc.parallelize(range(10))

def increment(x):
    global counter
    counter += x

rdd.foreach(increment)
print("Counter value: ", counter.value)

