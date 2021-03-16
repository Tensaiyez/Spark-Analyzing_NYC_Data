#!/usr/bin/env python

import sys
from csv import reader
from pyspark import SparkContext

sc=SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
lines_sec = sc.textFile(sys.argv[2], 1)
lines_sec = lines_sec.mapPartitions(lambda x: reader(x))
parking = lines.map(lambda x: (x[0], (x[14], x[6], x[2], x[1])))
open = lines_sec.map(lambda x: (x[0], ('', '', '', '')))
info=parking.join(open)
data=parking.subtractByKey(info).map(lambda x: x[0] + '\t' + x[1][0] + ', ' + x[1][1] + ', ' + x[1][2] + ', ' + x[1][3])
data.saveAsTextFile("task1.out")
sc.stop()
