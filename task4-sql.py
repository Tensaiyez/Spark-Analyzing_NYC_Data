import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task4-sql").config("spark.some.config.option", "some-value").getOrCreate()
parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
output = spark.sql("SELECT 'NY' AS us_state, count(registration_state) AS num_state FROM parking WHERE registration_state='NY' UNION SELECT 'OTHERS' as us_state, count(registration_state) FROM parking WHERE NOT registration_state='NY' ORDER BY us_state")


output.select(format_string('%s\t%d',output.us_state, output.num_state)).write.save("task4-sql.out",format="text")
