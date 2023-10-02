"""
In PySpark, the unionByName() function is widely used as the transformation to merge or union two DataFrames
with the different number of columns (different schema) by passing the allowMissingColumns with the value true.
The important difference between unionByName() function and the union() function is that this function resolves columns
by the name (and not by the position).In other words, the unionByName() function is used to merge two DataFrame’s
by the column names instead of by the position.

"""

# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit

# COMMAND ----------

# Implementing the unionByName() function in Databricks in PySpark

spark = SparkSession.builder.appName('unionByName() PySpark').getOrCreate()

# Creating dataframe1
sample_data = [("Ram", "Sales", 44), ("Shyam", "Sales", 46),
               ("Amit", "Sales", 40), ("Rahul", "Finance", 34)]
sample_columns = ["name", "dept", "age"]
dataframe1 = spark.createDataFrame(data=sample_data, schema=sample_columns)
dataframe1.printSchema()

# Creating dataframe2
sample_data2 = [("Ram", "Sales", "RJ", 8000), ("Shyam", "Finance", "DL", 8000),
                ("Amit", "Finance", "RJ", 8900), ("Rahul", "Marketing", "DL", 9000)]
sample_columns2 = ["name", "dept", "state", "salary"]
dataframe2 = spark.createDataFrame(data=sample_data2, schema=sample_columns2)
dataframe2.printSchema()

# Adding missing columns 'state' & 'salary' to dataframe1
for column in [column for column in dataframe2.columns if column not in dataframe1.columns]:
    dataframe1 = dataframe1.withColumn(column, lit(None))

# Adding missing column 'age' to dataframe2
for column in [column for column in dataframe1.columns if column not in dataframe2.columns]:
    dataframe2 = dataframe2.withColumn(column, lit(None))

# Merging two dataframe's dataframe1 & dataframe2 by name
merged_dataframe = dataframe1.unionByName(dataframe2)
merged_dataframe.show()