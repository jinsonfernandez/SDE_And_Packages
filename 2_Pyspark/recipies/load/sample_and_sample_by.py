"""
n PySpark, the sampling (pyspark.sql.DataFrame.sample()) is the widely used mechanism to get the random sample records
from the dataset and it is most helpful when there is a larger dataset and the analysis or test of the subset of the data
is required that is for example 15% of the original file. The syntax of the sample() file is "sample(with replacement,
fraction, seed=None)" in which "fraction" is defined as the Fraction of rows to generate in the range [0.0, 1.0]
and it doesn’t guarantee to provide the exact number of a fraction of records.
The "seed" is used for sampling (default a random seed) and is further used to reproduce the same random sampling.
The "with replacement" is defined as the sample with the replacement or not (default False).
The sample() function is defined as the function which is widely used to get Stratified sampling in PySpark
without the replacement. Further, it returns the sampling fraction for each stratum and if the stratum is not specified,
it takes the zero as the default.
"""

# Synatx: sample(with replacement, fraction, seed=None)
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# Implementing the sample() function and sampleBy() function in Databricks in PySpark

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("sample() and sampleBy() PySpark") \
    .getOrCreate()

dataframe = spark.range(100)
print(dataframe.sample(0.06).collect())

# Using sample() function
print(dataframe.sample(0.1,123).collect())

print(dataframe.sample(0.1,123).collect())

print(dataframe.sample(0.1,456).collect())

# Using the withReplacement(May contain duplicates)
## With Duplicates
print(dataframe.sample(True,0.3,123).collect())

## Without Duplicates
print(dataframe.sample(0.3,123).collect())

# Using sampleBy() function
dataframe2 = dataframe.select((dataframe.id % 3).alias("key"))

print(dataframe2.sampleBy("key", {0: 0.1, 1: 0.2},0).collect())
