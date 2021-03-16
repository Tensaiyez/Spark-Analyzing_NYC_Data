import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3-sql").config("spark.some.config.option", "some-value").getOrCreate()
open = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
open.createOrReplaceTempView("open")
output=spark.sql("SELECT license_type,CAST(SUM(amount_due)AS float) AS result,CAST(AVG(amount_due)AS float) AS ave FROM open GROUP BY license_type")
output.select(format_string("%s\t%.2f, %.2f",output.license_type, output.result, output.ave)).write.save("task3-sql.out",format="text")


