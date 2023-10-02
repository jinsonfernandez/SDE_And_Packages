"""
The StructType and the StructField classes in PySpark are popularly used to specify the schema to the DataFrame programmatically
 and further create the complex columns like the nested struct, array, and map columns.
 The StructType in PySpark is defined as the collection of the StructField’s that further defines the column name,
 column data type, and boolean to specify if field and metadata can be nullable or not.
 The StructField in PySpark represents the field in the StructType.
 An Object in StructField comprises of the three areas that are, name (a string), dataType (a DataType),
 and the nullable (a bool), where the field of the word is the name of the StructField.
"""

# Importing packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col,struct,when

# Implementing the StructType and StructField in Databricks in PySpark
spark = SparkSession.builder.master("local[1]") \
.appName('StructType and StructField') \
.getOrCreate()

# Creating StructType and StructField on dataframe
Sample_data = [("Ram","","Aggarwal","45458","M",4000),
("Shyam","Gupta","","45698","M",5000),
("Vijay","","Pandit","42365","M",5000),
("Roshni","Singh","kaur","36987","F",5000),
("Ishwar","Goel","Brown","","F",-2)
]

Sample_schema = StructType([ \
StructField("firstname",StringType(),True), \
StructField("middlename",StringType(),True), \
StructField("lastname",StringType(),True), \
StructField("id", StringType(), True), \
StructField("gender", StringType(), True), \
StructField("salary", IntegerType(), True) \
])

dataframe = spark.createDataFrame(data = Sample_data, schema = Sample_schema)
dataframe.printSchema()
dataframe.show(truncate=False)


# Nested StructType
Structure_Data = [
(("Ram","","Aggarwal"),"45458","M",4100),
(("Shyam","Gupta",""),"45698","M",5300),
(("Vijay","","Pandit"),"42365","M",2400),
(("Roshni","Singh","Kaur"),"36987","F",6500),
(("Ishwar","Goel","Brown"),"","F",-2)
]

Structure_Schema = StructType([
StructField('name', StructType([
    StructField('firstname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lastname', StringType(), True)])),
StructField('id', StringType(), True),
StructField('gender', StringType(), True),
StructField('salary', IntegerType(), True)
])
dataframe2 = spark.createDataFrame(data = Structure_Data, schema = Structure_Schema)
dataframe2.printSchema()
dataframe2.show(truncate=False)

# Updating struct of a dataframe using struct() function
Updated_DF = dataframe2.withColumn("OtherInfo",
                                   struct(
                                       col("id").alias("identifier"),
                                       col("gender").alias("gender"),
                                       col("salary").alias("salary"),
                                       when(col("salary").cast(IntegerType()) < 3000,"Low")
                                       .when(col("salary").cast(IntegerType()) < 4000,"Medium")
                                       .otherwise("High").alias("SalaryGrade"))
                                   ).drop("id","gender","salary")
Updated_DF.printSchema()
Updated_DF.show(truncate=False)
