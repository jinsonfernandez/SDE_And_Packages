"""
In PySpark, the DataFrame.fillna() or DataFrameNaFunctions.fill() functions is used to replace the NULL or None values
on all of the selected multiple DataFrame columns with the either zero(0), empty string, space, or any constant literal values.
The fill() and fill() functions are used to replace null/none values with an empty string, constant value and the zero(0)
on the Dataframe columns integer, string with Python.
The PySpark Dataframe is a distributed collection of the data organized into the named columns and is conceptually
equivalent to the table in the relational database or the data frame in Python or R language
"""

# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# Implementing the fillna() function and fill() function in Databricks in PySpark

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("fillna() and fill() PySpark") \
    .getOrCreate()

file_Path = "/FileStore/tables/smallzipcode.csv"
dataframe = spark.read.options(header='true', inferSchema='true') \
          .csv(file_Path)

dataframe.printSchema()
dataframe.show(truncate=False)

# Using fillna() and fill() functions
dataframe.fillna(value=0).show()
dataframe.fillna(value=0,subset=["population"]).show()
dataframe.na.fill(value=0).show()
dataframe.na.fill(value=0,subset=["population"]).show()

# Replace Null/None value with empty string
dataframe.fillna(value="").show()
dataframe.na.fill(value="").show()

# Replacing column type with the empty string
# And column city with value “unknown”.
dataframe.fillna("unknown",["city"]) \
    .fillna("",["type"]).show()