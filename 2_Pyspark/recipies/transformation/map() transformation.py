"""
 the map (map()) is defined as the RDD transformation that is widely used to apply the transformation function (Lambda)
 on every element of Resilient Distributed Datasets(RDD) or DataFrame and further returns a new Resilient Distributed Dataset(RDD).
  The RDD map() transformation is also used to apply any complex operations like adding the column, updating the column,
  transforming data etc so the output of the map() transformations would always have the same number of records as the input.
  The DataFrame doesn’t have the map() transformation to use with the DataFrame, hence it is needed to convert the DataFrame
  to the RDD first. Further, If a heavy initialization is there,

  it is recommended to use the PySpark mapPartitions() transformation instead of the map() as with the mapPartitions()
  heavy initialization executes only once for each partition instead of every record.
"""

# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# Implementing the map() transformation in Databricks in PySpark

spark = SparkSession.builder.master("local[1]") \
    .appName("map() PySpark").getOrCreate()

Sample_data = ["Project","Narmada","Gandhi","Adventures",
"in","Gujarat","Project","Narmada","Adventures",
"in","Gujarat","Project","Narmada"]

Rdd = spark.sparkContext.parallelize(Sample_data)

# Using map() transformation
Rdd2 = Rdd.map(lambda x: (x,1))
for element in Rdd2.collect():
    print(element)