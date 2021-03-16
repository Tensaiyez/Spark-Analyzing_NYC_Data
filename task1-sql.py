import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task1-sql").config("spark.some.config.option", "some-value").getOrCreate()
parkViol = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
openViol = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2])

parkViol.createOrReplaceTempView("parkViol")
openViol.createOrReplaceTempView("openViol")

output = spark.sql("SELECT P.summons_number, P.plate_id, P.violation_precinct, P.violation_code, P.issue_date FROM parkViol P LEFT JOIN openViol OP ON P.summons_number = OP.summons_number WHERE OP.summons_number is NULL") 

output.select(format_string('%d\t%s, %d, %d, %s',output.summons_number,output.plate_id, output.violation_precinct, output.violation_code, date_format(output.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out", format="text")
