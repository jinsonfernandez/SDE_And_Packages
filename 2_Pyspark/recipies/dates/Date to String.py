# Importing package
import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_spark_object(envn, appName):
    try:
        print(f"get_spark_object has started for environment {envn}")

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


spark = get_spark_object('TEST', 'date_to_string')
df = spark.createDataFrame([['l']], ['id'])
df.printSchema()

df_formatted = df.select(
    F.current_date().alias('now'),
    F.date_format(F.current_date(), "yyyy MM dd").alias("yyyy MM dd"),
    F.date_format(F.current_timestamp(), "MM/dd/yyyy hh:mm").alias("MM/dd/yyyy hh:mm"),
    F.date_format(F.current_timestamp(), "yyyy MMM dd F").alias("yyyy MMM dd F") )





