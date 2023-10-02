import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

def get_spark_object(envn, appName):
    try:
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession \
        .builder \
        .config("spark.jars", "/usr/local/postgresql-42.2.5.jar") \
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

spark = get_spark_object('TEST', 'Write to Postgres')

studentDf = spark.createDataFrame([
    Row(id=1,name='vijay',marks=67),
    Row(id=2,name='Ajay',marks=88),
    Row(id=3,name='jay',marks=79),
    Row(id=4,name='vinay',marks=67), ])

studentDf.printSchema()
# studentDf.show()

studentDf.select("id","name","marks").write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/dezyre_new") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "students") \
    .option("user", "hduser").option("password", "bigdata").save()