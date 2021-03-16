import sys
from csv import reader
from pyspark import SparkContext

sc=SparkContext()
lines=sc.textFile(sys.argv[1],1)
lines = lines.mapPartitions(lambda x: reader(x))

lines=lines.map(lambda x: ((x[14],x[16]),1))
lines=lines.reduceByKey(lambda x,y:x+y).takeOrdered(1,key =lambda x:-x[1])
result=sc.parallelize(lines)
ans=result.map(lambda x: x[0][0] + ', ' + x[0][1] + '\t' + str(x[1]))
ans.saveAsTextFile("task5.out")
sc.stop()
