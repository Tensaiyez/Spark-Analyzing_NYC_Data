import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task7-sql").config("spark.some.config.option", "some-value").getOrCreate()
parking =spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
output= spark.sql("SELECT one.violation_code, two.endOfweek, one.num FROM (SELECT violation_code, CAST(COUNT(summons_number)AS float)/8 AS endOfweek FROM parking WHERE DAY(issue_date)IN (5,6,12,13,19,20,26,27) GROUP BY violation_code) AS two RIGHT OUTER JOIN (SELECT violation_code,CAST(COUNT(summons_number)AS float) AS num FROM parking GROUP BY violation_code) AS one ON two.violation_code = one.violation_code ORDER BY one.violation_code")
output = output.fillna(0)
output.select(format_string("%d\t%.2f,%.2f",output.violation_code,output.endOfweek,(output.num - output.endOfweek)/23)).write.save("task7-sql.out",format="text")

