import sys
from csv import reader
from pyspark import SparkContext

if __name__=="__main__":


	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))
	lines=lines.map(lambda x:x[16])
	
	def state_check(x):
		if x=='NY':
			return ('NY',1)
		else:
			return ('Other',1)

	NewYork=lines.map(lambda x:state_check(x))

	data=NewYork.reduceByKey(lambda x,y:x+y).map(lambda x:x[0]+"\t"+str(x[1]))
	data.saveAsTextFile("task4.out")
	sc.stop()
