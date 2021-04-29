
# 1.首先，引入所需要使用的类
from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import IndexToString, StringIndexer

spark = SparkSession.\
    builder.\
    config(conf = SparkConf()).getOrCreate()



# 2.其次，构建1个DataFrame，设置StringIndexer的输入列和输出列的名字。
df = spark.createDataFrame([(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],["id", "category"])
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")

# 3.然后，通过fit()方法进行模型训练，用训练出的模型对原数据集进行处
# 理，并通过indexed.show()进行展示。
model = indexer.fit(df)
indexed = model.transform(df)

# IndexToString的作用是把标签索引的一列重新映射回原有的字符型标签
toString = IndexToString(inputCol="categoryIndex", outputCol="originalCategory")
indexString = toString.transform(indexed)
indexString.select("id", "originalCategory").show()
print(indexed.show())
