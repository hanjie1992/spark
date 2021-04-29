
def spark_create_rdd_set():
    """从集合中创建RDD"""
    from pyspark import SparkContext, SparkConf
    # 创建SparkConf并设置App名称
    conf = SparkConf().\
        setMaster("local").\
        setAppName("spark")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 从集合中创建RDD，Spark主要提供了两种函数：
    # parallelize和makeRDD(pyspark没有)
    spark_rdd = sc.parallelize([1,2,3,4,5],2)
    print(spark_rdd.count())
    # collect()以数组的形式返回数据集
    print(spark_rdd.collect())
    # glom允许将分区视为数组而不是单个行
    print(spark_rdd.glom().collect())
    print("获取分区数：",spark_rdd.getNumPartitions())
    sc.stop()

def spark_create_rdd_local():
    """外部本地创建"""
    from pyspark import SparkContext, SparkConf
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local").setAppName("spark")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf = conf)
    # 读取文件
    result = sc.textFile("../data/data.txt").\
        flatMap(lambda line: line.split(" ")).\
        map(lambda word: (word, 1)).\
        reduceByKey(lambda a, b: a + b).collect()
    print(result)
    sc.stop()

def spark_create_rdd_hdfs():
    """外部HDFS创建，本地python版本与远程端一致"""
    from pyspark import SparkContext, SparkConf
    """
    参数	说明
    local	使用一个Worker线程本地化运行Spark（默认）
    local[k]	使用K个Worker线程本地化运行Spark
    local[*]	使用K个Worker线程本地化运行Spark(这里K自动设置为机器的CPU核数)
    spark://HOST:PORT	连接到指定的Spark单机版集群(Spark standalone cluster)master。
                        必须使用master所配置的接口，默认接口7077.如spark://10.10.10.10:7077
    mesos://HOST:PORT	连接到指定的Mesos集群。host参数是Moses master的hostname。
                        必须使用master所配置的接口，默认接口是5050
    yarn-client	以客户端模式连接到yarn集群，集群位置由环境变量HADOOP_CONF_DIR决定.
    yarn-cluster	以集群模式连接到yarn集群，同样由HADOOP_CONF_DIR决定连接
    """
    conf = SparkConf()\
        .setMaster("spark://hadoop101:7077")\
        .setAppName("spark")
    sc = SparkContext(conf=conf)
    result = sc.textFile("hdfs://hadoop101:8020/input/")\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b).collect()
    sc.stop()
    print(result)

if __name__ == "__main__":
    # spark_create_rdd_set()
    spark_create_rdd_local()
    # spark_create_rdd_hdfs()
    pass
