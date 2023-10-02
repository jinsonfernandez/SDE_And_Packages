"""
The JSON functions in Apache Spark are popularly used to query or extract elements from the JSON string of the DataFrame
column by the path and further convert it to the struct, map type e.t.c.

The from_json() function in PySpark is converting
the JSON string into the Struct type or Map type.

The to_json() function in PySpark is defined as to converts the
MapType or Struct type to JSON string.

The json_tuple() function in PySpark is defined as extracting the Data from JSON and
then creating them as the new columns.
"""

# Importing packages
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, to_json, col
from pyspark.sql.functions import json_tuple, get_json_object
from pyspark.sql.functions import schema_of_json, lit

spark = SparkSession.builder.master("local[1]").appName("JSON").getOrCreate()
Sample_Json_String = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
dataframe = spark.createDataFrame([(1, Sample_Json_String)],["id","value"])
dataframe.show(truncate=False)

dataframe2 = dataframe.withColumn("value", from_json(dataframe.value,MapType(StringType(), StringType())))
dataframe2.printSchema()
dataframe2.show(truncate=False)
# Using to_json() function
dataframe2.withColumn("value", to_json(col("value"))) \
.show(truncate=False)

# Using json_tuple() function
dataframe.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")) \
.toDF("id","Zipcode","ZipCodeType","City") \
.show(truncate=False)
# Using get_json_object() function
dataframe.select(col("id"), get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
.show(truncate=False)

# Using schema_of_json() function
Schema_Str = spark.range(1) \
.select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""))) \
.collect()[0][0]
print(Schema_Str)

