
def top_n():
    """求Top N个payment值
    字段说明：orderid,userid,payment,productid
    """
    from pyspark import SparkConf, SparkContext
    conf = SparkConf().setMaster("local")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("../data/topn/file*")
    result1 = lines.filter(lambda line: (len(line.strip()) > 0) and (len(line.split(",")) == 4))
    result2 = result1.map(lambda x: x.split(",")[2])
    result3 = result2.map(lambda x: (int(x), ""))
    result4 = result3.repartition(1)
    result5 = result4.sortByKey(False)
    result6 = result5.map(lambda x: x[0])
    result7 = result6.take(5)
    for a in result7:
        print(a)

if __name__ == "__main__":
    top_n()
    pass
