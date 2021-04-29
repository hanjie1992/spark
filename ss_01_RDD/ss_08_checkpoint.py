from pyspark import SparkContext, SparkConf
def rdd_checkpoint():
    # 创建SparkConf
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize(["b", "a", "c"],1)
    #  需要设置路径，新建文件夹，否则抛异
    sc.setCheckpointDir("./our")
    #　增加缓存,避免再重新跑一个job做checkpoint
    rdd.cache()
    #数据检查点
    rdd.checkpoint()
    print(rdd.collect())

if __name__ == "__main__":
    rdd_checkpoint()
    pass