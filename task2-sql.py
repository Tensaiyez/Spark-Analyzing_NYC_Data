import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2-sql").config("spark.some.config.option", "some-value").getOrCreate()
parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

parking.createOrReplaceTempView("parking")

output=spark.sql("SELECT violation_code, COUNT(summons_number) AS ans FROM parking GROUP BY violation_code")
output.select(format_string("%d\t%d",output.violation_code, output.ans)).write.save("task2-sql.out",format="text")


