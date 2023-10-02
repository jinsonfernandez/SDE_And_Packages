"""
The PySpark kurtosis() function calculates the kurtosis of a column in a PySpark DataFrame, which measures the degree of
outliers or extreme values present in the dataset.
A higher kurtosis value indicates more outliers, while a lower one indicates a flatter distribution.
"""
# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import kurtosis
from pyspark.sql.functions import min, max, mean

# COMMAND ----------

# Implementing the kurtosis(), min(), max() and mean() functions in Databricks in PySpark

spark = SparkSession.builder.appName('PySpark kurtosis(), min(), max() and mean()').getOrCreate()

Sample_Data = [("Rahul", "Technology", 8000),
               ("Prateek", "Finance", 7600),
               ("Ram", "Sales", 5100),
               ("Reetu", "Marketing", 4000),
               ("Himesh", "Sales", 2000),
               ("Shyam", "Finance", 3500),
               ("Harsh", "Finance", 4900),
               ("Ramesh", "Marketing", 4000),
               ("Raina", "Marketing", 3000),
               ("Ankit", "Sales", 5100)
               ]
Sample_schema = ["employee_name", "department", "salary"]

dataframe = spark.createDataFrame(data=Sample_Data, schema=Sample_schema)
dataframe.printSchema()
dataframe.show(truncate=False)

# Using kurtosis function
dataframe.select(kurtosis("salary")).show(truncate=False)

# Using max() function
dataframe.select(max("salary")).show(truncate=False)

# Using min() function
dataframe.select(min("salary")).show(truncate=False)

# Using mean() function
dataframe.select(mean("salary")).show(truncate=False)