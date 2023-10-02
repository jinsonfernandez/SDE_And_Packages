import pyspark
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

spark = get_spark_object('TEST', 'timestamp_to_date')

dataframe = spark.createDataFrame(
data = [ ("1","2021-08-26 11:30:21.000")],
schema=["id","input_timestamp"])
dataframe.printSchema()

dataframe = dataframe.withColumn("date_type", F.to_date(F.current_timestamp())) \
    .withColumn("timestamp_type", F.to_timestamp(F.col("input_timestamp")))


dataframe.printSchema()
dataframe.show()



