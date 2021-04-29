
# 1.引入所需要的类，并构建数据集。
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.linalg import Vector, Vectors

spark = SparkSession.\
    builder.\
    config(conf = SparkConf()).getOrCreate()

df = spark.createDataFrame(
    [(Vectors.dense(-1.0, 1.0, 1.0),),
    (Vectors.dense(-1.0, 3.0, 1.0),),
    (Vectors.dense(0.0, 5.0, 1.0), )],
    ["features"])

#２.构建VectorIndexer转换器，设置输入和输出列，并进行模型训练。
indexer = VectorIndexer(inputCol="features", outputCol="indexed", maxCategories=2)
indexerModel = indexer.fit(df)

#3.通过VectorIndexerModel的categoryMaps成员来获得被转换的特征及其映射，这里可以
# 看到，共有两个特征被转换，分别是0号和2号。
categoricalFeatures = indexerModel.categoryMaps.keys()
print ("Choose"+str(len(categoricalFeatures))+ "categorical features:"+str(categoricalFeatures))

# 4.把模型应用于原有的数据，并打印结果。
indexed = indexerModel.transform(df)
print(indexed.show())
