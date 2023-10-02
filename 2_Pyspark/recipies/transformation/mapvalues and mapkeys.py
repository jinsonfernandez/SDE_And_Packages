# The PySpark MapType (also called map type) in Apache Spark is popularly known as the data type, used to
# represent the Python Dictionary (dict) for storing the key-value pair. The MapType object comprises of the
# three fields which are key type (a DataType), valueType (a DataType) and the valueContainsNull (a BooleanType).
# The PySpark MapType represents the Map key-value pair similar to the python Dictionary (Dict). It extends the
# DataType class, which is the superclass of all the types in the PySpark, which takes the two mandatory
# arguments: key type and value type of type DataType and one optional boolean argument that is valueContainsNull.
# The map_values() function is used to get all the map values. The map_keys() function is used to get all map keys.

# Importing packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, MapType
from pyspark.sql.types import StringType, MapType
from pyspark.sql.functions import map_values
from pyspark.sql.functions import map_keys

spark = SparkSession.builder.appName('PySpark map_values() and map_keys()').getOrCreate()
Sample_schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])

Sample_dataDictionary = [
    ('Ram',{'hair':'brown','eye':'brown'}),
    ('Shyam',{'hair':'black','eye':'black'}),
    ('Raman',{'hair':'orange','eye':'black'}),
    ('Sonu',{'hair':'red','eye':None}),
    ('Vinay',{'hair':'black','eye':''})
]

dataframe = spark.createDataFrame(data = Sample_dataDictionary, schema = Sample_schema)
dataframe.printSchema()
dataframe.show(truncate=False)

# Using map_values() function
dataframe.select(dataframe.name, map_values(dataframe.properties)).show()
# Using map_keys() function
dataframe.select(dataframe.name, map_keys(dataframe.properties)).show()

