# 首先我们先取其中的后两类数据，用二项逻辑斯蒂回归进行二分类分析
# １．导入本地向量Vector和Vectors，导入所需要的类
from pyspark.ml.linalg import Vector, Vectors
from pyspark.sql import Row, functions
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, HashingTF, Tokenizer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, BinaryLogisticRegressionSummary, \
    LogisticRegression
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

"""
2.我们定制一个函数，来返回一个指定的数据，然后读取文本文件，
第一个map把每行的数据用“,”隔开，比如在我们的数据集中，每行被分成了5部分，
前4部分是鸢尾花的4个特征，最后一部分是鸢尾花的分类；我们这里把特征存储
在Vector中，创建一个Iris模式的RDD，然后转化成dataframe；
最后调用show()方法来查看一下部分数据。
"""

def f(x):
    rel = {}
    rel['features'] = Vectors. \
        dense(float(x[0]), float(x[1]), float(x[2]), float(x[3]))
    rel['label'] = str(x[4])
    return rel

spark = SparkSession. \
    builder. \
    config(conf=SparkConf()).getOrCreate()

data = spark.sparkContext. \
    textFile("../data/iris.txt"). \
    map(lambda line: line.split(',')). \
    map(lambda p: Row(**f(p))). \
    toDF()

print(data.show())

# 3.分别获取标签列和特征列，进行索引并进行重命名。
labelIndexer = StringIndexer().\
    setInputCol("label").\
    setOutputCol("indexedLabel").\
    fit(data)
featureIndexer = VectorIndexer(). \
    setInputCol("features").\
    setOutputCol("indexedFeatures").\
    fit(data)

"""
4.设置LogisticRegression算法的参数。这里设置了循环次数为100次，
规范化项为0.3等，具体可以设置的参数，可以通过explainParams()来获取，
还能看到程序已经设置的参数的结果。
"""
lr = LogisticRegression().\
    setLabelCol("indexedLabel").\
    setFeaturesCol("indexedFeatures").\
    setMaxIter(100).\
    setRegParam(0.3).\
    setElasticNetParam(0.8)
print("LogisticRegression parameters:\n" + lr.explainParams())

"""
5.设置一个IndexToString的转换器，把预测的类别重新转化成字符型的。
构建一个机器学习流水线，设置各个阶段。上一个阶段的输出将是本阶段的输入。
"""
labelConverter = IndexToString().\
    setInputCol("prediction").\
    setOutputCol("predictedLabel").\
    setLabels(labelIndexer.labels)

lrPipeline = Pipeline().\
    setStages([labelIndexer, featureIndexer, lr, labelConverter])


"""
6.把数据集随机分成训练集和测试集，其中训练集占70%。Pipeline本质上
是一个评估器，当Pipeline调用fit()的时候就产生了一个PipelineModel，
它是一个转换器。然后，这个PipelineModel就可以调用transform()来进
行预测，生成一个新的DataFrame，即利用训练得到的模型对测试集进行验证。
"""
trainingData, testData = data.randomSplit([0.7, 0.3])
lrPipelineModel = lrPipeline.fit(trainingData)
lrPredictions = lrPipelineModel.transform(testData)

"""
7.输出预测的结果，其中，select选择要输出的列，collect获取所有行
的数据，用foreach把每行打印出来。
"""
preRel = lrPredictions.select( "predictedLabel", "label", "features", "probability").collect()
for item in preRel:
    print(str(item['label'])+','+ str(item['features'])+
          '-->prob='+ str(item['probability'])+',predictedLabel'+ str(item['predictedLabel']))

"""
8.对训练的模型进行评估。创建一个MulticlassClassificationEvaluator实例，用setter方法
把预测分类的列名和真实分类的列名进行设置，然后计算预测准确率。
"""
evaluator = MulticlassClassificationEvaluator().\
    setLabelCol("indexedLabel").\
    setPredictionCol("prediction")
lrAccuracy = evaluator.evaluate(lrPredictions)
print(lrAccuracy)

"""
9.可以通过model来获取训练得到的逻辑斯蒂模型。lrPipelineModel是一个PipelineModel，
因此，可以通过调用它的stages方法来获取模型，具体如下：
"""
lrModel = lrPipelineModel.stages[2]
print ("Coefficients: \n " + str(lrModel.coefficientMatrix)+
       "\nIntercept: "+str(lrModel.interceptVector)+
       "\n numClasses: "+str(lrModel.numClasses)+
       "\n numFeatures: "+str(lrModel.numFeatures))