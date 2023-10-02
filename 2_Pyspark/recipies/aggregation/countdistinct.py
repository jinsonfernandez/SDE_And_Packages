# Importing packages
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
.appName('Spark Count Distinct') \
.getOrCreate()
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
Sample_columns = ["Name","Dept","Salary"]
dataframe = spark.createDataFrame(data = Sample_data, schema = Sample_columns)
dataframe.show()

# Using distinct().count() function
print("Distinct Count: " + str(dataframe.distinct().count()))
# Using countDistinct() function
dataframe2 = dataframe.select(F.countDistinct( F.col("Dept"), F.col("salary") ))
dataframe2.show()
