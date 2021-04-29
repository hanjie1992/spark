from pyspark import SparkContext

sc = SparkContext("local")
a = sc.broadcast([1, 2, 3, 4, 5])
rdd = sc.parallelize(["a"]).flatMap(lambda x: a.value).collect()
print(rdd)
rdd = a.unpersist()
print(rdd)

