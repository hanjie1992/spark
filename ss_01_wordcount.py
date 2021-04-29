from pyspark import SparkContext

sc = SparkContext('local')
lines = sc.textFile("./data/data.txt")
result = lines.\
    flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a + b)
print(result.collect())



