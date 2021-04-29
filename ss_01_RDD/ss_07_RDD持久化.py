from pyspark import SparkContext, SparkConf,StorageLevel

def rdd_cache():
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD相加
    rdd = sc.parallelize(["b", "a", "c"])
    rdd.cache()
    print(rdd.getStorageLevel())

def rdd_persist():
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD相加
    rdd = sc.parallelize(["b", "a", "c"])
    rdd.persist(storageLevel=StorageLevel(True, True, False, False, 2))
    # 查看是否持久化
    print(rdd.is_cached)
    # 查看当前的持久化级别
    print(rdd.getStorageLevel())
    #　取消持久化
    rdd = rdd.unpersist()
    print(rdd.is_cached)

if __name__ == "__main__":
    # rdd_cache()
    rdd_persist()
    pass