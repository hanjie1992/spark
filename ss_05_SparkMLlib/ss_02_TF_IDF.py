# 1.导入TF-IDF所需要的包：
from pyspark.ml.feature import HashingTF,IDF,Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.\
    builder.\
    config(conf = SparkConf()).getOrCreate()

# 2.创建一个简单的DataFrame，每一个句子代表一个文档
sentenceData = spark.createDataFrame([
    (0, "I heard about Spark and I love Spark"),
     (0, "I wish Java could use case classes"),
     (1, "Logistic regression models are neat")])\
    .toDF("label", "sentence")

# 3.得到文档集合后，即可用tokenizer对句子进行分词
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
print(wordsData.show())

# 4.得到分词后的文档序列后，即可使用HashingTF的transform()方法把句子哈希成
# 特征向量，这里设置哈希表的桶数为2000
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2000)
featurizedData = hashingTF.transform(wordsData)
print(featurizedData.select("words", "rawFeatures").show(truncate=False))

# 5.调用IDF方法来重新构造特征向量的规模，生成的变量idf是一个评估器，在特征向量
# 上应用它的fit()方法，会产生一个IDFModel（名称为idfModel）。
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)

# 6.调用IDFModel的transform()方法，可以得到每一个单词对应的TF-IDF度量值。
rescaledData = idfModel.transform(featurizedData)
print(rescaledData.select("features", "label").show(truncate=False))
