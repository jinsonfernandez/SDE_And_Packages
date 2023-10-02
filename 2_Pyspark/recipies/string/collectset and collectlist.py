"""
The Aggregate functions in Apache PySpark accept input as the Column type or the column name in the string and follow
several other arguments based on the process and returning the Column type.
The Aggregate functions operate on the group of rows and calculate the single return value for every group.
The PySpark SQL Aggregate functions are further grouped as the “agg_funcs” in the Pyspark.
The collect_set() function returns all values from the present input column with the duplicate values eliminated.
The collect_list() function returns all the current input column values with the duplicates.

"""

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.appName('PySpark collect_set() and collect_list()').getOrCreate()

Sample_Data = [("Rahul", "Technology", 8000),
               ("Prateek", "Finance", 7600),
               ("Ram", "Sales", 5100),
               ("Reetu", "Marketing", 4000),
               ("Himesh", "Sales", 2000),
               ("Shyam", 'Finance', 3500),
               ("Harsh", "Finance", 4900),
               ("Ramesh", "Marketing", 4000),
               ("Raina", "Marketing", 3000),
               ("Ankit", "Sales", 5100)
               ]
Sample_schema = ["employee_name", "department", "salary"]

dataframe = spark.createDataFrame(data=Sample_Data, schema=Sample_schema)
dataframe.printSchema()
dataframe.show(truncate=False)

# Using collect_set() function
dataframe.select(F.collect_set(F.col("salary"))).show(truncate=False)

# Using collect_list() function
dataframe.select(F.collect_list("salary")).show(truncate=False)
