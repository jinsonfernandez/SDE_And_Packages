"""
n PySpark, the flatMap() is defined as the transformation operation which flattens the Resilient Distributed Dataset or
DataFrame(i.e. array/map DataFrame columns) after applying the function on every element and further returns
the new PySpark Resilient Distributed Dataset or DataFrame.
The PySpark Dataframe is a distributed collection of data organized into the named columns


Functionality:
    map(): Applies a transformation function to each element of an RDD and returns a new RDD consisting of the transformed elements.
    It produces one output element for each input element.
    flatMap(): Applies a transformation function to each element of an RDD and returns a new RDD by flattening the resulting elements.
    It can generate zero or more output elements for each input element.

Output Elements:
    map(): Produces one output element for each input element.
    The number of elements in the resulting RDD is the same as the original RDD.
    flatMap(): Can generate zero or more output elements for each input element.
    The resulting RDD may have a different number of elements than the original RDD.
    Structure of RDD:

Structure of RDD:
    map(): Preserves the original structure of the RDD. Each input element is transformed into a single output element,
    maintaining the RDD's structure.
    flatMap(): Flattens the output by merging the generated elements into a single RDD.
    The resulting RDD does not preserve the original structure, as multiple elements can be generated for each input element.
"""

# Databricks notebook source
# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# Implementing the flatmap() transformation in Databricks in PySpark

spark = SparkSession.builder.appName('flatmap() PySpark').getOrCreate()

sample_data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]

Rdd = spark.sparkContext.parallelize(sample_data)
for element in Rdd.collect():
    print(element)

# Using Flatmap() Transformation
Rdd2 = Rdd.flatMap(lambda x: x.split(" "))
for element in Rdd2.collect():
    print(element)
