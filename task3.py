import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))

linesVal = lines.map(lambda x: (x[2], float(x[12]))).reduceByKey(lambda x, y: x+y)

num = lines.map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x + y)

avgViol= linesVal.join(num)

violation=avgViol.map(lambda x: (x[0], x[1][0],float(x[1][0])/float(x[1][1])))

ans = violation.map(lambda x: x[0] + '\t' + "{0:.2f}".format(x[1]) + ', ' + "{0:.2f}".format(x[2]))
ans.saveAsTextFile("task3.out")
