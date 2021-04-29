from pyspark import SparkContext, SparkConf

def action_reduce():
    """该操作相当于MapReduce中的Reduce操作，可以
    通过指定的聚合方法来对RDD中元素进行聚合。"""
    from operator import add
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD相加
    rdd = sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
    print(rdd)

def action_collect():
    """返回一个包含RDD的所有元素的list"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD相加
    rdd = sc.parallelize([1,2]).collect()
    print(rdd)

def action_count():
    """返回RDD中的元素个数"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([1,2,5,3]).count()
    print(rdd)

def action_take():
    """返回由RDD前n个元素组成的list数组"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd1 = sc.parallelize([2, 3, 4, 5, 6]).cache().take(2)
    rdd2 = sc.parallelize([2, 3, 4, 5, 6]).take(10)
    rdd3 = sc.parallelize(range(100), 1).filter(lambda x: x > 90).take(3)
    print(rdd1,rdd2,rdd3)

def action_first():
    """返回RDD中的第一个元素"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd1 = sc.parallelize([2, 3, 4]).first()
    # rdd2 = sc.parallelize([]).first()
    print(rdd1)
    # print(rdd2)

def action_top():
    """返回RDD中元素的前n个最大值"""
    # 创建SparkConf
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([1,2,3,4,6,7,8]).top(3)
    print(rdd)

def action_saveAsTextFile():
    """将RDD中的元素以字符串的格式存储在文件系统中"""
    # 创建SparkConf
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    # rdd = sc.parallelize(range(10)).saveAsTextFile('hdfs://hadoop101:9000/out')
    rdd = sc.parallelize(range(10)).saveAsTextFile('./out')
    print(rdd)

def action_foreach():
    """遍历RDD中每一个元素"""
    # 创建SparkConf
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    def f(x): print(x)
    rdd = sc.parallelize([1, 2, 3, 4, 5]).foreach(f)
    print(rdd)

def action_foreachPartition():
    """遍历RDD的每个分区，针对每个分区执行f函数操作，可
    类比mapPartitions()。"""
    # 创建SparkConf
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    def f(iterator):
        for x in iterator:
            print(x)
    rdd = sc.parallelize([1, 2, 3, 4, 5]).foreachPartition(f)
    # 创建RDD
    print(rdd)

def action_countByKey():
    """统计每个Key键的元素数"""
    # 创建SparkConf
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    rdd = sorted(rdd.countByKey().items())
    print(rdd)

def action_collectAsMap():
    """与collect相关的算子是将结果返回到driver端。
    collectAsMap算子是将Kev-Value结构的RDD收集到driver端，
    并返回成一个字典"""
    # 创建SparkConf
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    m = sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
    print(type(m))
    print(m)
    n = sc.parallelize([(1, 2), (3, 4)]).collect()
    print(type(n))

def example_case():
    """
    题目：给定一组键值对("spark",2),("hadoop",6),("hadoop",4),("spark",6)，
    键值对的key表示图书名称，value表示某天图书销量，请计算每个键对应的平均值，
    也就是计算每种图书的每天平均销量。
    """
    # 创建SparkConf
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)])
    rdd = rdd.mapValues(lambda x:(x,1)).\
        reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))\
        .mapValues(lambda x:x[0]/x[1]).collect()
    print(rdd)

if __name__ == "__main__":
    # action_reduce()
    # action_collect()
    # action_count()
    # action_take()
    # action_first()
    # action_top()
    # action_saveAsTextFile()
    action_foreach()
    # action_foreachPartition()
    # action_countByKey()
    # action_collectAsMap()
    # example_case()
    pass
