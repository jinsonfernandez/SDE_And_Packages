"""
The row_number() function and the rank() function in PySpark is popularly used for day-to-day operations and make the
difficult task an easy way. The rank() function is used to provide the rank to the result within the window partition,
and this function also leaves gaps in position when there are ties. The row_number() function is defined as which gives
the sequential row number starting from the 1 to the result of each window partition.

"""

# Importing packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import row_number

# Implementing therank and row_number window functions in Databricks in PySpark
spark = SparkSession.builder.appName('Spark rank() row_number()').getOrCreate()

Sample_data = [("Ram", "Technology", 4000),
("Shyam", "Technology", 5600),
("Veer", "Technology", 5100),
("Renu", "Accounts", 4000),
("Ram", "Technology", 4000),
("Vijay", "Accounts", 4300),
("Shivani", "Accounts", 4900),
("Amit", "Sales", 4000),
("Anupam", "Sales", 3000),
("Anas", "Technology", 5100)
]

Sample_columns= ["employee_name", "department", "salary"]
dataframe = spark.createDataFrame(data = Sample_data, schema = Sample_columns)
dataframe.printSchema()
dataframe.show(truncate=False)

# Defining row_number() function
Window_Spec = Window.partitionBy("department").orderBy("salary")
dataframe.withColumn("row_number",row_number().over(Window_Spec)) \
.show(truncate=False)

# Defining rank() function
dataframe.withColumn("rank",rank().over(Window_Spec)) \
.show()
