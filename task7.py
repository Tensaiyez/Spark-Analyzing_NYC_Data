import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
wkday=lines.map(lambda x: (x[2],(0 if int(x[1][-2:]) in (5,6,12,13,19,20,26,27)  else 1)))
wkday=wkday.reduceByKey(lambda x, y: x + y)
wkend=lines.map(lambda x: (x[2],(1 if int(x[1][-2:]) in (5,6,12,13,19,20,26,27) else 0)))
wkend=wkend.reduceByKey(lambda x, y: x + y)
wkday=wkday.map(lambda x: (x[0], float(x[1])/23))
wkend=wkend.map(lambda x: (x[0], float(x[1])/8))
union=wkend.join(wkday)
result= union.map(lambda x: str(x[0]) + '\t' + "{0:.2f}".format(x[1][0]) + ', ' + "{0:.2f}".format(x[1][1]))
result.saveAsTextFile("task7.out")

