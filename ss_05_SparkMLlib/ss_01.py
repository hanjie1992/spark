# 1.引入要包含的包并构建训练数据集
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.\
    builder.\
    config(conf = SparkConf()).getOrCreate()
# Prepare training documents from a list of (id, text, label) tuples.
training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])

"""
2.定义 Pipeline 中的各个流水线阶段PipelineStage，包括转换器
和评估器，具体地，包含tokenizer, hashingTF和lr。
"""
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)

"""
3.按照具体的处理逻辑有序地组织PipelineStages，并创建一个Pipeline。
"""
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

"""
现在构建的Pipeline本质上是一个Estimator，在它的fit()方法运行之后，
它将产生一个PipelineModel，它是一个Transformer。
"""
model = pipeline.fit(training)
# 可以看到，model的类型是一个PipelineModel，这个流水线模型将在测试数据的时候使用

# 4. 构建测试数据
test = spark.createDataFrame([
    (4, "spark i j k"),
    (5, "l m n"),
    (6, "spark hadoop spark"),
    (7, "apache hadoop")
], ["id", "text"])

"""
5.调用之前训练好的PipelineModel的transform()方法，让测试数据按顺序
通过拟合的流水线，生成预测结果
"""
prediction = model.transform(test)
selected = prediction.select("id", "text", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row
    print("({0}, {1}) --> prob={2}, prediction={3}" .format(rid, text, str(prob), prediction))

