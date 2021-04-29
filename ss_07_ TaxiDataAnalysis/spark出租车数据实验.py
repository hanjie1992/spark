
'''
创建DataFrame
引入与sql相关的包初始化Spark上下文
'''
from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext()
''' as sc
使用textFile函数读取csv文件创建taxi_data，然后使用map算子操作将按照逗号隔开的文本创建RDD。
'''
taxi_data = sc.textFile("../data/taxi.csv")
taxi_rdd = taxi_data.map(lambda line: line.split(','))

'''
创建矢量RDD，矢量两个参数分别为纬度和经度。在下文的聚类函数中需要该格式RDD进行聚类。
'''
from pyspark.ml.linalg import Vectors

taxi_row = taxi_rdd.map(lambda x: (Vectors.dense(x[1], x[2]),))
'''
使用SparkSession创建sql上下文并使用createDataFrame创建DataFrame
'''
sqlsc = SparkSession.builder.getOrCreate()
taxi_df = sqlsc.createDataFrame(taxi_row, ["features"])
'''
输入和输出列是用户使用kmeans方法时候的参数类型，默认的输入列名为features，输出的列名
为prediction。KMeans方法中包含若干参数，其中k为分簇个数，seed为种子点。
'''
# from pyspark.ml.clustering import KMeans
#
# kmeans = KMeans(k=3, seed=1)
# model = kmeans.fit(taxi_df)
# centers = model.clusterCenters()
# print(centers)

from pyspark.mllib.clustering import KMeans
kmeans = KMeans()
model = kmeans.train(rdd=taxi_df,k=3,seed=1)
centers = model.clusterCenters()
print(centers)