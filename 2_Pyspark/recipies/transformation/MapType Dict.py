# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col, explode, map_keys, map_values
from pyspark.sql.types import StructType, StructField, StringType, MapType

# COMMAND ----------

# Implementing the MapType datatype in PySpark in Databricks

spark = SparkSession.builder.appName('Conversion of PySpark RDD to Dataframe PySpark').getOrCreate()

# Creating MapType 
map_Col = MapType(StringType(),StringType(),False)

# Creating MapType from StructType
sample_schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])

Data_Dictionary = [
        ('Ram',{'hair':'brown','eye':'blue'}),
        ('Shyam',{'hair':'black','eye':'black'}),
        ('Amit',{'hair':'grey','eye':None}),
        ('Aupam',{'hair':'red','eye':'black'}),
        ('Rahul',{'hair':'black','eye':'grey'})
        ]
dataframe = spark.createDataFrame(data = Data_Dictionary, schema = sample_schema)
dataframe.printSchema()
dataframe.show(truncate=False)

# Accessing PySpark MapType elements
dataframe3 = dataframe.rdd.map(lambda x: \
    (x.name,x.properties["hair"],x.properties["eye"])) \
    .toDF(["name","hair","eye"])
dataframe3.printSchema()
dataframe3.show()

#  Using getItem() function of Column type
dataframe.withColumn("hair",dataframe.properties.getItem("hair")) \
  .withColumn("eye",dataframe.properties.getItem("eye")) \
  .drop("properties") \
  .show()

dataframe.withColumn("hair",dataframe.properties["hair"]) \
  .withColumn("eye",dataframe.properties["eye"]) \
  .drop("properties") \
  .show()

# Using explode() function in MapType
dataframe.select(dataframe.name,explode(dataframe.properties)).show()

# Using map_keys() function in MapType
dataframe.select(dataframe.name,map_keys(dataframe.properties)).show()

# Using map_values() function in MapType
dataframe.select(dataframe.name,map_values(dataframe.properties)).show()