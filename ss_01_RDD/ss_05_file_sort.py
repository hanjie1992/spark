from pyspark import SparkContext, SparkConf

index=0
def getindex():
  global index
  index+=1
  return index

def file_sort():
  conf = SparkConf().setMaster("local").setAppName("FileSort")
  sc = SparkContext(conf = conf)
  lines= sc.textFile("../data/FileSort/file*")
  index = 0
  result1 = lines.filter(lambda line:(len(line.strip()) > 0))
  result2=result1.map(lambda x:(int(x.strip()),""))
  result3=result2.repartition(1)
  result4=result3.sortByKey(True)
  result5=result4.map(lambda x:x[0])
  result6=result5.map(lambda x:(getindex(),x))
  result6.foreach(print)
  result6.saveAsTextFile("../data/FileSort/sortresult")
if __name__ == "__main__":
    file_sort()
    pass
