import pyspark
import spark as spark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_spark_object(envn, appName):
    try:
        if envn == 'TEST':
            master = 'lcoal'
        else:
            master = 'yarn'

        spark = SparkSession \
        .builder \
        .master(master) \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

    except NameError as exp:
        print(f"There is name error in the method -> get_spark_object -> {str(exp)}", exc_info=True)
    except Exception as exp:
        print(f"There is an error in the method -> get_spark_object -> {str(exp)}", exc_info=True)
    else:
        print("Spark Object is Created.. ")
    return spark

spark = get_spark_object('TEST', 'translate and substring')
df= spark.sql("show databases")
df.show()

datafile = spark.read.format('csv') \
    .options(header='True', inferschema='true', sep=",",timestampFormat="yyyy-MM-dd") \
    .load("C:\\Users\\jfernandez\\Desktop\\JDC\data\\revenue.csv")

datafile.write.saveAsTable("drivers_table")

df1=spark.sql("select * from drivers_table limit 5")
df1.show()
df1.printSchema()







