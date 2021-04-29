from graphframes import GraphFrame
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()

vertices = spark.createDataFrame([
 		("a", "Alice", 34),
			("b", "Bob", 36)]
		  , ["id", "name", "age"])
edges = spark.createDataFrame([
			("a", "b", "friend")]
		  , ["src", "dst", "relationship"])

graph = GraphFrame(vertices,edges)
print(graph.vertices.show())