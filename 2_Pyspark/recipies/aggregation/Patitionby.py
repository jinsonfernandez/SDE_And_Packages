# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# Implementing the Partitionby() function in Databricks in PySpark

spark = SparkSession.builder.appName('Partitionby() PySpark').getOrCreate()

dataframe = spark.read.option("header",True) \
        .csv("/FileStore/tables/zipcodes.csv")
dataframe.printSchema()

# Using Partitionby() function
dataframe.write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/tmp/zipcodesState")

# Using Partitionby() function into Multiple columns
dataframe.write.option("header",True) \
        .partitionBy("state","city") \
        .mode("overwrite") \
        .csv("/tmp/zipcodesState")

# Using PartitionBy() function to control number of partitions
dataframe.write.option("header",True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/tmp/zipcodesState")