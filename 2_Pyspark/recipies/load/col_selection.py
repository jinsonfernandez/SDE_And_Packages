# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# Implementing the selection of columns from DataFrame in Databricks in PySpark

spark = SparkSession.builder.appName('Select Column PySpark').getOrCreate()

sample_data = [("Ram","Gupta","India","Delhi"),
    ("Shyam","Aggarwal","India","Delhi"),
    ("Amit","Kabuliwala","India","Uttar Pradesh"),
    ("Babu","Dabbawala","India","Rajasthan")]

sample_columns = ["firstname","lastname","country","state"]
dataframe = spark.createDataFrame(data = sample_data, schema = sample_columns)
dataframe.show(truncate=False)

# Selecting Single column and Multiple columns
dataframe.select("firstname").show()
dataframe.select("firstname","lastname").show()

#Using Dataframe object name to select column
dataframe.select(dataframe.firstname, dataframe.lastname).show()

# Using col function
dataframe.select(col("firstname"),col("lastname")).show()

# Selecting the Nested Struct Columns in PySpark
sample_data1 = [(("Rame",None,"Gupta"),"Rajasthan","M"),
        (("Anita","Garg",""),"Delhi","F"),
        (("Pooja","","Aggarwal"),"Delhi","F"),
        (("Saurabh","Anne","Jones"),"Jammu","M"),
        (("Shahrukh","Khan","Brown"),"Maharashtra","M"),
        (("Salman","Gupta","Williams"),"Delhi","M")
        ]

sample_schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])

dataframe2 = spark.createDataFrame(data = sample_data1, schema = sample_schema)
dataframe2.printSchema()
dataframe2.show(truncate=False) # shows all columns

dataframe2.select("name").show(truncate=False)
dataframe2.select("name.firstname","name.lastname").show(truncate=False)
dataframe2.select("name.*").show(truncate=False)