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

Sample_address = [(1,"15861 Bhagat Singh","RJ"),
                  (2,"45698 Ashoka Road","DE"),
                  (3,"23654 Laxmi Nagar","Bi")]

dataframe = spark.createDataFrame(Sample_address,["id", "address", "state"])
dataframe.printSchema()

dataframe = spark.createDataFrame([("FGHIJ_WSY", "HIJ")], ("col1", "col2"))
dataframe.select(F.overlay("col1", "col2", 8).alias("overlayed_column"))





