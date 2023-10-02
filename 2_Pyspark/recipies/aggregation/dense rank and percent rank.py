"""
The dense_rank() function and the percent_rank() function in PySpark is popularly used for day-to-day operations
and make the difficult task an easy way. The dense_rank() window function in PySpark is defined to be used to
get the result with the rank of rows within the window partition without any gaps that is it is similar
to the rank() function, just the difference being rank() function leaves gaps in rank when there are ties.
The percent_rank() function in PySpark is defined to return the status of rows in the percentage format with windows specified.
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import percent_rank

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
               ("Anas", "Technology", 5100)]

Sample_columns = ["employee_name", "department", "salary"]
dataframe = spark.createDataFrame(data=Sample_data, schema=Sample_columns)
dataframe.printSchema()

Window_Spec = Window.partitionBy("department").orderBy("salary")
dataframe.withColumn("dense_rank",dense_rank().over(Window_Spec)).show()

# Defining percent_rank() function
dataframe.withColumn("percent_rank",percent_rank().over(Window_Spec)) \
.show()


