"""
The Translate() function in Apache PySpark translates any character that matches the given matchString(Defined by the user) in the column by the already given replaceString.
It helps in replacing character by nature of the given Dataframe column value.
Translate(Column, String, String) were three parameters "Column" which corresponds to Column that is to be applied,
"String" which corresponds to matchString that is String to be matched,
and "String" which corresponds to replaceString that is String to be replaced with. It returns the Column object.

The Substring() function in Apache PySpark is used to extract the substring from a DataFrame string column on the provided position and the length of the string defined by the user.
The substring() function can be used with the select() function and selectExpr() function to get the substring of the column(date) as the year, month, and day.
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

spark = get_spark_object('TEST', 'translate and substring')

Sample_address = [(1,"15861 Bhagat Singh","RJ"),
                  (2,"45698 Ashoka Road","DE"),
                  (3,"23654 Laxmi Nagar","Bi")]

dataframe =spark.createDataFrame(Sample_address,["id","address","state"])
# Using the translate() function
dataframe_translate = dataframe.withColumn("address_correctec", F.translate(F.col("address"), '234', 'DEF'))

#Defining data for subsring function
Sample_data = [(1,"30654128"),(2,"36985215")]
Sample_columns = ["id","date"]
dataframe1 = spark.createDataFrame(Sample_data, Sample_columns)

# Using the Substring() function with select() function
dataframe2 = dataframe1.select( F.substring(F.col('date'),2,4).alias('year'), \
    F.substring(F.col("date"),6,3).alias("month"), \
        F.substring(F.col("date"),8,3).alias("day"))

# Using SelectExpr and Substring
dataframe2 = dataframe1.selectExpr("substring(date, 2, 4) as year", \
                                   "substring(date, 3,6) as month", \
                                   "substring(date, 8,3) as day"
                                   )

# Using WithColumn
dataframe2 = dataframe1.withColumn("year", F.substring(F.col("date"), 2, 4)) \
    .withColumn("month", F.substring(F.col("date"), 6, 2)) \
    .withColumn("day", F.substring(F.col("date"), 8, 2))





