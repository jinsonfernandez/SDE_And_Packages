"""
Used with Aggregate function
The first() function returns the first element present in the column, when the ignoreNulls is set to True, it returns the first non-null element.
The last() function returns the last element present in the column, when ignoreNulls is set to True, it further returns the last non-null element.
"""

from pyspark.sql import  SparkSession
import pyspark.sql.functions as F

def get_spark_object(envn, appName):
    try:
        print(f"get_spark_object() has started. The {envn} env is used")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()
    except NameError as exp:
        print(f"There is name error in the method -> get_spark_object -> {str(exp)}", exc_info=True)
    except Exception as exp:
        print(f"There is an error in the method -> get_spark_object -> {str(exp)}", exc_info=True)
    else:
        print("Spark Object is Created.. ")
    return spark

spark = get_spark_object('TEST', 'translate and substring')

Sample_Data = [("Rahul", "Technology", 8000),
                ("Prateek", "Finance", 7600),
                ("Ram", "Sales", 5100),
                ("Reetu", "Marketing", 4000),
                ("Himesh", "Sales", 2000),
                ("Shyam", "Finance", 3500),
                ("Harsh", "Finance", 4900),
                ("Ramesh", "Marketing", 4000),
                ("Raina", "Marketing", 3000),
                ("Ankit", "Sales", 5100)]

Sample_schema = ["employee_name", "department", "salary"]
dataframe = spark.createDataFrame(data = Sample_Data, schema = Sample_schema)
dataframe.printSchema()

dataframe.select(F.first(F.col("salary")) )
dataframe.select(F.last(F.col("salary")) )




