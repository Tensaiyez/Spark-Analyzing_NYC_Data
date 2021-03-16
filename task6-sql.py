import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task6-sql").config("spark.some.config.option", "some-value").getOrCreate()
parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
output = spark.sql("SELECT plate_id, registration_state, COUNT(summons_number) AS ans FROM parking GROUP BY plate_id, registration_state ORDER BY ans DESC LIMIT 20")
output.select(format_string("%s, %s\t%d",output.plate_id,output.registration_state, output.ans)).write.save("task6-sql.out",format="text")

