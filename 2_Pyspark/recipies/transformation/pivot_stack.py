"""
the pivot() function is defined as the most important function and used to rotate or transpose the data from one column
into the multiple Dataframe columns and back using the unpivot() function.
The Pivot() function is an aggregation where one of the grouping columns values is transposed into the individual columns
with the distinct data.
"""

# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# Implementing the Pivot() function and Stack() function in Databricks in PySpark

spark = SparkSession.builder.appName('Pivot() Stack() PySpark').getOrCreate()

sample_data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

sample_columns= ["Product","Amount","Country"]
dataframe = spark.createDataFrame(data = sample_data, schema = sample_columns)
dataframe.printSchema()
dataframe.show(truncate=False)

# Using the pivot() function
pivot_DataFrame = dataframe.groupBy("Product").pivot("Country").sum("Amount")
pivot_DataFrame.printSchema()
pivot_DataFrame.show(truncate=False)

pivot_DataFrame2 = dataframe.groupBy("Product","Country") \
      .sum("Amount") \
      .groupBy("Product") \
      .pivot("Country") \
      .sum("sum(Amount)")
pivot_DataFrame2.printSchema()
pivot_DataFrame2.show(truncate=False)

# Using stack() function to unpivot
unpivot_Expr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
unpivot_DataFrame = pivot_DataFrame2.select("Product", expr(unpivot_Expr)) \
    .where("Total is not null")
unpivot_DataFrame.show(truncate=False)