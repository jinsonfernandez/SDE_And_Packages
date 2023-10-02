# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.functions import sumDistinct
from pyspark.sql.functions import variance, var_samp, var_pop

# COMMAND ----------

# Implementing the sum(), sumDistinct(), variance(), var_samp() and var_pop() functions in Databricks in PySpark

spark = SparkSession.builder.appName('PySpark sum() sumDistinct() variance() var_samp() and var_pop()').getOrCreate()

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

# Using sum() function
dataframe.select(sum("salary")).show(truncate=False)

# Using sumDistinct() function
dataframe.select(sumDistinct("salary")).show(truncate=False)

# Using variance(), var_samp() and var_pop() functions
dataframe.select(variance("salary"), var_samp("salary"), var_pop("salary")) \
    .show(truncate=False)