"""
ArrayType is widely used and is defined as the collection data type that extends the DataType class which is
the superclass of all types in the PySpark.
All elements of ArrayType should have the same type of elements.

The explode() function of ArrayType is used to create the new row for each element in the given array column.

The split() SQL function as an ArrayType function returns an array type after splitting the string column
by the delimiter.

Further, the array() ArrayType function is used to create a new array column by merging the data from multiple columns.
 Further, All the input columns must have the same data type.

 The array_contains() SQL function is further used to check if the array column contains a value and
 it returns null if the array is null, true if the array contains the value, and false otherwise.
"""

# Importing packages
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType,StructType,StructField
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import array
from pyspark.sql.functions import array_contains

spark = SparkSession.builder.master("local[1]").appName("array_type").getOrCreate()
# Implementing the ArrayType functions in Databricks in PySpark
arrayCol = ArrayType(StringType(),False)

Sample_data = [
    ("Rahul,, Gupta",["C","C++","Python"],["Spark","C"],"RJ","DL"),
    ("Manan,, Aggarwal",["Spark","C","C++"],["Spark","C"],"MH","UK"),
    ("Hemant,, Singh",["Scala","Go"],["Spark","Matlab"],"AP","JH")
]

Sample_schema = StructType([
    StructField("Name",StringType(),True),
    StructField("Languages_at_School",ArrayType(StringType()),True),
    StructField("Languages_at_Home",ArrayType(StringType()),True),
    StructField("Current_State", StringType(), True),
    StructField("Previous_Travelled_State", StringType(), True)
])

dataframe = spark.createDataFrame(data = Sample_data, schema = Sample_schema)
# Using explode() function
dataframe.select(dataframe.Name, explode(dataframe.Languages_at_School)).show()

# Using explode() function
dataframe.select(dataframe.Name, explode(dataframe.Languages_at_School)).show()

# Using split() function
dataframe.select(split(dataframe.Name,",").alias("NameAsArray")).show()

# Using array() function
dataframe.select(dataframe.Name,array(dataframe.Current_State, dataframe.Previous_Travelled_State).alias("States")).show()

# Using array_contains() function
dataframe.select(dataframe.Name, array_contains(dataframe.Languages_at_School,"C") \
.alias("Array_Contains")).show()

