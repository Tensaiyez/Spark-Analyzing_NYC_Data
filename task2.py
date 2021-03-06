#!/usr/bin/env python

import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
code=lines.map(lambda x: (x[2],1))
data=code.reduceByKey(lambda x,y: x+y).map(lambda x: x[0] + '\t' + str(x[1]))
data.saveAsTextFile("task2.out")
sc.stop()
