import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

envn_list = ['TEST', 'PROD']
envn = envn_list[0]

try:
    if envn == 'TEST':
        master = 'local'
    else:
        master = 'yarn'

    spark = SparkSession \
        .builder \
        .master(master) \
        .appName("Pyspark_filter") \
        .getOrCreate()
except Exception as exp:
    print(exp)

schooldriver_Schema = T.StructType([
    T.StructField("school_year", T.StringType())
    , T.StructField("vendor_name", T.StringType())
    , T.StructField("type_of_service", T.StringType())
    , T.StructField("active_employees", T.StringType())
    , T.StructField("job_type", T.StringType())
])

# Create Dataframe from Streaming
schooldrivers_stream_Df = spark.readStream.format("csv") \
    .option(header=True, inferSchema=True, sep=',', timestampFormat="yyyy-MM-dd") \
    .load("/home/hduser/school_drives",schema=schooldriver_Schema)

schooldrivers_stream_Df = schooldrivers_stream_Df.withColumn(F.col("school_year"), F.to_date(F.col("school_year")))

filtered_df = schooldrivers_stream_Df.filter(
    (F.col("school_year").between(F.lit("2021"), F.lit("2023"))) &
    (
            F.col("vendor_name").contains("bus") |
            (F.col("job_type") == "active")
    ) )

# Start the streaming query
streaming_query = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

streaming_query.awaitTermination()
