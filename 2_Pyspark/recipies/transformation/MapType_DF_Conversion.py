"""
The create_map() function in Apache Spark is popularly used to convert the selected or all the DataFrame columns to the MapType,
 similar to the Python Dictionary (Dict) object.
 The create_map(column) function takes input as the list of columns grouped as the key-value pairs
 (key1, value1, key2, value2, key3, value3…)
 and which has to be converted using the function. The create_map() function returns the MapType column.
 The create_map() function is the PySpark SQL function which is imported from the "pyspark.sql.functions".
"""

# Importing package
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, create_map

spark = SparkSession.builder.master("local[1]").appName("MapType").getOrCreate()

Sample_data = [("38874", "Technology", 5000, "IND"),
               ("42105", "Technology", 6000, "BHU"),
               ("46987", "Finance", 4900, "IND"),
               ("35412", "Entertainment", 3500, "ISR"),
               ("36987", "Finance", 5500, "IND")]

Sample_schema = StructType([
    StructField('id', StringType(), True),
    StructField('dept', StringType(), True),
    StructField('salary', IntegerType(), True),
    StructField('location', StringType(), True)
])

dataframe = spark.createDataFrame(data=Sample_data, schema=Sample_schema)
dataframe.printSchema()

# Convert columns to Map
dataframe = dataframe.withColumn(
    "PropertiesOnMap", create_map(
        lit("salary"), col("salary"),
        lit("location"), col("location"))
).drop("salary", "location")

dataframe.printSchema()
dataframe.show(truncate=False)
