"""
The date diff() function in Apache PySpark is popularly used to get the difference of dates and the number of days
between the dates specified. The "datediff(date, date)" is the syntax of the datediff() function where the
first argument specifies the input of the Date and the Second argument specifies an additional Date argument
from which the first argument is differentiated. The months_between() function in Apache PySpark is popularly
used to get the difference of dates and the number of months between the dates specified.
The "months_between(date, date)" is the syntax of the months_between() function where the first argument specifies
the input of the Date and the Second argument specifies an additional Date argument from which the first
argument is differentiated.
"""

import pyspark
from pyspark.sql import  SparkSession
import pyspark.sql.functions as F

def get_spark_object(envn, appName):
    try:
        print(f"get_spark_object() has started. The {envn} env is used")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()

    except NameError as exp:
        print(f"There is name error in the method -> get_spark_object -> {str(exp)}", exc_info=True)
    except Exception as exp:
        print(f"There is an error in the method -> get_spark_object -> {str(exp)}", exc_info=True)
    else:
        print("Spark Object is Created.. ")

    return spark

spark = get_spark_object('TEST', 'date_calc')

df = spark.createDataFrame([['l']], ['id'])
df.printSchema()



df_formatted = df.select(
    F.current_date().alias('now'),
    F.date_format(F.current_date(), "yyyy MM dd").alias("yyyy MM dd"),
    F.date_format(F.current_timestamp(), "MM/dd/yyyy hh:mm").alias("MM/dd/yyyy hh:mm"),
    F.date_format(F.current_timestamp(), "yyyy MMM dd F").alias("yyyy MMM dd F")
)

data = [("1","2021-04-01"),("2","2021-06-07"),("3","2021-08-09")]
dataframe = spark.createDataFrame(data = data,schema=["id","date"])

data = [("1","2021-04-01"),("2","2021-06-07"),("3","2021-08-09")]
dataframe = spark.createDataFrame(data = data,schema=["id","date"])

# Using datediff() function
dataframe.select(
    F.col("date"),
    F.current_date().alias("Current_Date"),
    F.datediff(F.current_date(),F.col("date")).alias("datediff")
).show()

# Using months_between() function
dataframe.withColumn("Dates_Diff", F.datediff(F.current_date(),F.col("date"))) \
    .withColumn("Months_Diff", F.months_between(F.current_date(),F.col("date"))) \
    .withColumn("Months_Diff_round",round(F.months_between(F.current_date(),F.col("date")),3)) \
    .withColumn("Years_Diff",F.months_between(F.current_date(),F.col("date"))/F.lit(12)) \
    .withColumn("Years_Diff_round",round(F.months_between(F.current_date(),F.col("date"))/F.lit(12),3)) \
    .show()

# Defining data in other than default
# PySpark date format
data2 = [("1","01-04-2021"),("2","07-06-2021"),("3","09-08-2021")]
dataframe2 = spark.createDataFrame(data=data2,schema=["id","date"])

# Converting the data into default PySpark data format
dataframe2.select(
    F.to_date(F.col("date"),"MM-dd-yyyy").alias("date"),
    F.current_date().alias("endDate")
)

