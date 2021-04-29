from pyspark import SparkContext, SparkConf

def transformation_map1():
    """
    map(func)操作将每个元素传递到函数func中，并将结果返回为一个新的数据集
    """
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1, 2, 3, 4, 5], 2)
    rdd = rdd.map(lambda x: x + 10)
    print(rdd.collect())


def transformation_map2():
    """
    map(func)操作将每个元素传递到函数func中，并将结果返回为一个新的数据集
    """
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("../data/word.txt")
    rdd = rdd.map(lambda line: line.split(" "))
    print(rdd.collect())


def transformation_flatMap():
    """
    将函数应用于该RDD的所有元素，然后将结果平坦化(压扁)，从而返回新的RDD
    """
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("../data/word.txt")
    rdd = rdd.flatMap(lambda line: line.split(" "))
    print(rdd.collect())

def transformation_filter():
    """
    接收一个返回值为布尔类型的函数作为参数。当某个RDD调用filter方法时，
    会对该RDD中每一个元素应用f函数，如果返回值类型为true，则该元素会
    被添加到新的RDD中。
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建一个RDD
    rdd = sc.parallelize([1,2,3,4],2)
    # 打印过滤后生成的新RDD
    rdd = rdd.filter(lambda x : x%2==0)
    print(rdd.glom().collect())

def transformation_interserction():
    """
    返回这个RDD和另一个RDD的交集，输出将不包含任何重复的元素
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建两个RDD
    rdd1 = sc.parallelize(range(1,6),1)
    rdd2 = sc.parallelize(range(4,8),1)
    # 计算两个RDD的并集
    rdd = rdd1.intersection(rdd2)
    # 打印到控制台
    print(rdd.glom().collect())
    # 关闭连接
    sc.stop()

def transformation_union():
    """
    对源RDD和参数RDD求并集后返回一个新的RDD
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建两个RDD
    rdd1 = sc.parallelize(range(1,4),1)
    rdd2 = sc.parallelize(range(4,8),1)
    # 计算两个RDD的并集
    rdd = rdd1.union(rdd2)
    # 打印到控制台
    print(rdd.glom().collect())
    # 关闭连接
    sc.stop()

def transformation_distinct():
    """
    distinct(numPartitions=None) 默认情况下，distinct会生成与
    原RDD分区个数一致的分区数。可以去重后修改分区个数。
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建一个RDD
    rdd = sc.parallelize([1,2,1,5,2,9,6,1],4)
    # 打印去重后生成的新RDD
    rdd = rdd.distinct(2)
    print(rdd.glom().collect())
    sc.stop()


def transformation_sortBy1():
    """
    sortBy(ascending=True, numPartitions=None)
    ascending=True 默认为正序排列
    该操作用于排序数据。在排序之前，可以将数据通过f函数进行处理，
    之后按照f函数处理的结果进行排序，默认为正序排列。排序后新产生
    的RDD的分区数与原RDD的分区数一致。
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([2, 1, 3, 4, 6, 5], 1)
    # 正序
    # rdd = rdd.sortBy(lambda x : x)
    # 倒序
    rdd = rdd.sortBy(lambda x: x, False)
    # 打印到控制台
    print(rdd.glom().collect())
    # 关闭连接
    sc.stop()


def transformation_sortBy2():
    """
    sortBy(ascending=True, numPartitions=None)
    ascending=True 默认为正序排列
    该操作用于排序数据。在排序之前，可以将数据通过f函数进行处理，
    之后按照f函数处理的结果进行排序，默认为正序排列。排序后新产生
    的RDD的分区数与原RDD的分区数一致。
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    tmp = [('a', 1), ('b', 4), ('d', 5), ('c', 2), ('e', 3)]
    rdd = sc.parallelize(tmp, 1)
    # 元祖第一列正序排序
    # rdd = rdd.sortBy(lambda x : x[0])
    # 元祖第二列倒序排序
    rdd = rdd.sortBy(lambda x: x[1], False)
    # 打印到控制台
    print(rdd.glom().collect())
    # 关闭连接
    sc.stop()

def transformation_mapPartitions():
    """
    mapPartitions(f,  preservesPartitioning=False)
	f：函数把每一个分区的数据分别放入到送代器中，批处理
	preservesPartitioning：是否保那上游RDD的分区信息，默认false
    功能说明：
	Map是一次处理一个元素，而mapPartitions一次处理一个分区数据。
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建一个RDD
    rdd = sc.parallelize([1,2,3,4],2)
    # 先把yield看做“return”
    def f(iterator): yield sum(iterator)
    rdd = rdd.mapPartitions(f)
    # rdd = rdd.mapPartitions(lambda x : x)
    # rdd = rdd.map(lambda x:x*2)
    print(rdd.glom().collect())

def transformation_mapPartitionsWithIndex():
    """
    类似于mapPartitions，比mapPartitions多一个整数参数表示分区号
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建一个RDD
    rdd = sc.parallelize([1, 2, 3, 4], 2)
    def f(splitIndex, iterator): yield splitIndex, list(iterator)
    rdd = rdd.mapPartitionsWithIndex(f).collect()
    print(rdd)



def transformation_subtract():
    """
    返回在RDD1中出现，但是不在RDD2中出现的元素，不去重
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建两个RDD
    rdd1 = sc.parallelize(range(1,5),1)
    rdd2 = sc.parallelize(range(3,8),1)
    # 计算两个RDD的差集
    rdd = rdd1.subtract(rdd2)
    # 打印到控制台
    print(rdd.glom().collect())
    # 关闭连接
    sc.stop()



def transformation_partitionBy():
    """
    针对Key-Value结构的RDD重新分区，采用默认的Hash分区
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建一个3分区的RDD
    rdd = sc.parallelize([(1,"aaa"),(2,"bbb"),(3,"ccc")],3)
    # 重新设置为2个分区
    rdd = rdd.partitionBy(2).glom().collect()
    print(rdd)

def transformation_mapValues():
    """
    在不改变原有Key键的基础上，对Key-Value结构RDD的Vaule值进行一个map操作，分区保持不变。
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    x = sc.parallelize([("a", ["apple", "banana", "lemon"]), ("b", ["grapes"])])
    def f(x): return len(x)
    # mapValues算子使用时，f()函数仅作用在Value值上，
    # Key键保持不变，返回的值为Key键和Value的长度。
    rdd = x.mapValues(f).collect()
    print(rdd)

def transformation_flatMapValues():
    """
    对Key-Value结构的RDD先执行mapValue操作，再执行压平的操作，
    类似map与flatMap的区别
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    x = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
    def f(x): return x
    rdd = x.flatMapValues(f).collect()
    print(rdd)

def transformation_reduceByKey1():
    """
    根据Key进行分组，对分组内的元素进行操作。输出分区数和指定分区数相同，
    如果没有指定分区数，安装默认的并行级别，默认分区规则是哈希分区。
    """
    from operator import add
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    rdd = sorted(rdd.reduceByKey(add).collect())
    print(rdd)

def transformation_reduceByKey2():
    """
    根据Key进行分组，对分组内的元素进行操作。输出分区数和指定分区数相同，
    如果没有指定分区数，安装默认的并行级别，默认分区规则是哈希分区。
    """
    from operator import add
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    pairRDD = sc.parallelize([("Hadoop", 1), ("Spark", 1), ("Hive", 1), ("Spark", 1)])
    pairRDD.reduceByKey(lambda a,b:a+b).foreach(print)

def transformation_groupByKey():
    """将Pair RDD中的相同Key的值放一个序列中。"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    rdd1 = sorted(rdd.groupByKey().mapValues(len).collect())
    rdd2 = sorted(rdd.groupByKey().mapValues(list).collect())
    print(rdd1,rdd2)

def transformation_sortByKey():
    """将Pair RDD中的相同Key的值放一个序列中。"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
    rdd1 = sc.parallelize(tmp).sortByKey().first()
    rdd2 = sc.parallelize(tmp).sortByKey(True, 1).glom().collect()
    rdd3 = sc.parallelize(tmp).sortByKey(True, 2).glom().collect()
    print(rdd1,"===",rdd2,"+++",rdd3)
    tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
    tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
    sc.parallelize(tmp2).sortByKey(True, 3, keyfunc=lambda k: k.lower()).glom().collect()
    print("tmp2===",tmp2)

def transformation_keys():
    """返回一个RDD，元素为每个tuple中的key键"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]").setAppName("SparkCore")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    m = sc.parallelize([(1, 2), (3, 4)]).keys()
    rdd = m.collect()
    print(rdd)

def transformation_values():
    """返回一个RDD，元素为每个tuple中的value值。"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    m = sc.parallelize([(1, 2), (3, 4)]).values()
    rdd = m.collect()
    print(rdd)

def transformation_join1():
    """与SQL中的join含义一致，根据数据的Key键进行连接。
    在类型为（K，V）和（K，W）的RDD上调用，返回一个
    相同key对应的所有元素对在一起的（K，（V，W））的RDD。
    注意：如果Key只是某一个RDD有，这个Key不会关联
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    x = sc.parallelize([("a", 1), ("b", 4)])
    y = sc.parallelize([("a", 2), ("a", 3)])
    rdd = sorted(x.join(y).collect())
    print(rdd)

def transformation_join2():
    """与SQL中的join含义一致，根据数据的Key键进行连接。
    在类型为（K，V）和（K，W）的RDD上调用，返回一个
    相同key对应的所有元素对在一起的（K，（V，W））的RDD。
    注意：如果Key只是某一个RDD有，这个Key不会关联
    """
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([(1, "a"), (2, "b"), (3, "c")])
    rdd1 = sc.parallelize([(1, 4), (2, 5), (4, 6)])
    rdd = rdd.join(rdd1).collect()
    print(rdd)

def transformation_leftOuterJoin():
    """左外连接，与SQL中的左外连接一致"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    x = sc.parallelize([("a", 1), ("b", 4)])
    y = sc.parallelize([("a", 2)])
    rdd = sorted(x.leftOuterJoin(y).collect())
    print(rdd)

def transformation_rightOuterJoin():
    """右外连接，与SQL中的左外连接一致。"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    x = sc.parallelize([("a", 1), ("b", 4)])
    y = sc.parallelize([("a", 2)])
    rdd = sorted(x.rightOuterJoin(y).collect())
    print(rdd)

def transformation_glom():
    """该操作将RDD中每一个分区变成一个数组，并放置在新的RDD中，
    数组中元素的类型与原分区中元素类型一致。"""
    # 创建SparkConf并设置App名称
    conf = SparkConf().setMaster("local[*]")
    # 创建SparkContext，该对象是提交Spark App的入口
    sc = SparkContext(conf=conf)
    # 创建RDD
    rdd = sc.parallelize([1, 2, 3, 4], 2)
    rdd1 = sorted(rdd.collect())
    rdd2 = sorted(rdd.glom().collect())
    print(rdd1,"=====",rdd2)



if __name__ == "__main__":
    # transformation_map1()
    # transformation_map2()
    # transformation_flatMap()
    # transformation_filter()
    # transformation_union()
    # transformation_interserction()
    # transformation_distinct()
    # transformation_sortBy1()
    # transformation_sortBy2()
    # transformation_mapPartitions()
    # transformation_mapPartitionsWithIndex()
    # transformation_subtract()

    """Key-Value类型"""
    # transformation_partitionBy()
    # transformation_mapValues()
    # transformation_flatMapValues()
    # transformation_reduceByKey1()
    # transformation_reduceByKey2()
    # transformation_groupByKey()
    # transformation_sortByKey()
    # transformation_keys()
    # transformation_values()
    # transformation_join1()
    # transformation_join2()
    # transformation_leftOuterJoin()
    # transformation_rightOuterJoin()
    # transformation_glom()
    pass
