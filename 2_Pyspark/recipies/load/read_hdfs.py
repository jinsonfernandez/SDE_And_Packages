import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession \
    .builder \
    .master(master='local') \
    .appName('read_hdfs') \
    .getOrCreate()

bookSchema = T.StructType([ \
    T.StructField("id", T.IntegerType()), \
    T.StructField("book_title", T.StringType()), \
    T.StructField("publish_or_not", T.StringType()), \
    T.StructField("technology", T.StringType())])

bookdata = spark.read \
    .format('csv') \
    .options(header='True'
             , inferschema='true'
             , sep=","
             , timestampFormat="yyyy-MM-dd") \
    .load("hdfs://localhost:9000/dezyre_books", schema=bookSchema)
