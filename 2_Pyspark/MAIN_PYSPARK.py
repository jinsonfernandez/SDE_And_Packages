'''
                            SparkSession:
It is the entry point and central interface for interacting with Spark functionalities.
SparkSession provides a unified interface and handles the configuration, context creation,
and coordination of various Spark components.
It encapsulates the functionality previously provided by SparkContext, SQLContext,
and HiveContext in earlier versions of Spark.

SparkSession provides access to Spark's built-in modules such as SQL (Spark SQL), DataFrames, Datasets, and Streaming

                            SparkContext:
It is the core component of a Spark application and represents the connection to a Spark cluster.
SparkContext is responsible for coordinating the execution of tasks, managing distributed data,
and communicating with the cluster manager.
SparkContext provides low-level APIs for interacting with the Spark runtime,
including creating RDDs (Resilient Distributed Datasets), performing transformations and actions on RDDs,
managing cluster resources, and configuring various settings for Spark applications.
'''
# =====================================================================================================================
!pipinstallpyspark
!pipinstallpyarrow

spark - submit. / code / Ch03 / word_count_submit.py

# ================================================================================================================================================================

import pyspark
from pyspark.sql import SparkSession

spark = (SparkSession \
         .builder \
         .appName("Analyzing the vocabulary of Pride and Prejudice.") \
         .getOrCreate())
spark.sparkContext.setLogLevel("INFO")

revenue = spark.read.format('csv').options(header='True', inferschema='true', sep=",",
                                           timestampFormat="yyyy-MM-dd").load(
    "C:\\Users\\jfernandez\\Desktop\\JDC\data\\revenue.csv")
revenue.printSchema()
revenue.show(5, truncate=False)

from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    count,
    sum,
    avg,
    round)

new_df = revenue.withColumn("year", year("event_date")) \
    .withColumn("month", month("event_date")) \
    .withColumn("date", dayofmonth("event_date"))

new_df.to_pandas_on_spark().shape
install_non_null = new_df.filter(~(col("install_id") == ""))

new_df.filter((col("event_date") >= "2022-01-01") & (col("event_date") <= "2022-12-31")) \
    .groupBy(col("event_date")) \
    .agg(
    count(col("install_id")).alias("install_count"),
    round(sum(col("value_usd")), 2).alias("USD_Value")) \
    .orderBy(col("event_date"), col("USD_Value").desc()).show(10)
'''========================================================================================================================='''\

# A better way to import functions is to use an alias for function
import pyspark.sql.functions as F

new_df.filter(
    (F.col("event_date") >= "2022-01-01") & (F.col("event_date") <= "2022-12-31")
) \
    .groupBy(F.col("event_date")) \
    .agg(
    F.count(F.col("install_id")).alias("install_count"),
    F.round(F.sum(F.col("value_usd")), 2).alias("USD_Value")) \
    .orderBy(F.col("event_date"), F.col("USD_Value").desc()).show(10)
# =====================================================================================================================================================================

import pyspark.sql.functions as F

DIRECTORY = "./data/broadcast_logs"
logs = spark.read.csv(
    path=os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)
'''-------------------------------- Column Operations ----------------------------------------------------------'''
# Selecting Columns

# Using the string to column conversion
logs.select("BroadCastLogID", "LogServiceID", "LogDate")
logs.select(*["BroadCastLogID", "LogServiceID", "LogDate"])

# Passing the column object explicitly
logs.select(
    F.col("BroadCastLogID"), F.col("LogServiceID"), F.col("LogDate")
)
logs.select(
    *[F.col("BroadCastLogID"), F.col("LogServiceID"), F.col("LogDate")]
)

# Dropping Columns

logs = logs.drop("BroadcastLogID", "SequenceNO")
logs = logs.select(
    [x for x in logs.columns if x not in ["BroadcastLogID", "SequenceNO"]])

# Casting the columns
logs.select(
    F.col("Duration"),
    F.col("Duration").substr(1, 2).cast("int").alias("dur_hours"),
    F.col("Duration").substr(4, 2).cast("int").alias("dur_minutes"),
    F.col("Duration").substr(7, 2).cast("int").alias("dur_seconds"),
    (
            F.col("Duration").substr(1, 2).cast("int") * 60 * 60
            + F.col("Duration").substr(4, 2).cast("int") * 60
            + F.col("Duration").substr(7, 2).cast("int")
    ).alias("Duration_seconds"),
).distinct().show(5)

# USing withcolumn
logs = logs.withColumn(
    "Duration_seconds",
    (
            F.col("Duration").substr(1, 2).cast("int") * 60 * 60
            + F.col("Duration").substr(4, 2).cast("int") * 60
            + F.col("Duration").substr(7, 2).cast("int")
    ),
)

# Renaming the Columns
logs = logs.withColumnRenamed("Duration_seconds", "duration_seconds")
logs.toDF(*[x.lower() for x in logs.columns]).printSchema()

# Diagnosing a data frame with describe() and summary()
for i in logs.columns:
    logs.describe(i).show()

for i in logs.columns:
    logs.select(i).summary("min", "10%", "90%", "max").show()

'''-------------------------------------------------------------------------------------------------------------------'''

                                                # Joining and grouping
DIRECTORY = "./data/broadcast_logs"
log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

logs_and_channels = logs.join(
    log_identifier,
    on="LogServiceID",
    how="inner"
)

# name clashing in joins
logs_and_channels_verbose = logs.alias("left").join(
    log_identifier.alias("right"),
    logs["LogServiceID"] == log_identifier["LogServiceID"],
)

logs_and_channels_verbose.drop(F.col("right.LogServiceID")).select("LogServiceID")

(full_log
 .groupby("ProgramClassCD", "ProgramClass_Description")
 .agg(F.sum("duration_seconds").alias("duration_total"))
 .orderBy("duration_total", ascending=False).show(100, False)
 )

# Using case when
F.when(
    F.trim(F.col("ProgramClassCD")).isin(["COM", "PRC", "PGI", "PRO", "PSA", "MAG", "LOC", "SPO", "MER", "SOL"]),
    F.col("duration_seconds"), ).otherwise(0)

answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn("commercial_ratio", F.col("duration_commercial") / F.col("duration_total"))
)

answer.orderBy("commercial_ratio", ascending=False).show(1000, False)

# End to end Program
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Getting the Canadian TV channels with the highest/lowest proportion of commercials."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Reading all the relevant data sources

DIRECTORY = "./data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
)

log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)
cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),
)

cd_program_class = spark.read.csv(
    "./data/broadcast_logs/ReferenceTables/CD_ProgramClass.csv",
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)

# Data processing

logs = logs.drop("BroadcastLogID", "SequenceNO")

logs = logs.withColumn(
    "duration_seconds",
    (
            F.col("Duration").substr(1, 2).cast("int") * 60 * 60
            + F.col("Duration").substr(4, 2).cast("int") * 60
            + F.col("Duration").substr(7, 2).cast("int")
    ),
)

log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

logs_and_channels = logs.join(log_identifier, "LogServiceID")

full_log = logs_and_channels.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)

answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio", F.col("duration_commercial") / F.col("duration_total")
    )
    .fillna(0)
)

answer.orderBy("commercial_ratio", ascending=False).show(1000, False)

# =================================================================================================================================================================

# 6 Multidimensional data frames: Using PySpark with JSON data

{                              ❶
"id": 143, ❷
"name": "Silicon Valley",
"type": "Scripted",
"language": "English",
"genres": [                  ❸
"Comedy"
],
"network": {                 ❹
"id": 8,
"name": "HBO",
"country": {
    "name": "United States",
    "code": "US",
    "timezone": "America/New_York"
}
},
"_embedded": {
    "episodes": [              ❺
{
    "id": 10897,
    "name": "Minimum Viable Product",
    "season": 1,
    "number": 1,
},
{
    "id": 10898,
    "name": "The Cap Table",
    "season": 1,
    "number": 2,
}
]
}
}

'''
❶ At thetoplevel, aJSON bject looks like a Python dictionary.
    Both use the brackets to delimit object boundaries.

❷ JSON data is encoded into key - value pairs, just like in a dictionary.\
    JSON keys must be strings.

❸ JSON arrays can contain multiple values(here, we have a single string).

❹ Objects can be values too;
you can nest objects within one another this way.

❺ Our episodes are each objects contained within an array.

# +--------------+--------+
# |name          |genres  |
# +--------------+--------+
# |Silicon Valley|[Comedy]|
# +--------------+--------+
'''
import pyspark.sql.functions as F

array_subset = array_subset.select(
    "name",
    array_subset.genres[0].alias("dot_and_index"),           ❶
F.col("genres")[0].alias("col_and_index"),
array_subset.genres.getItem(0).alias("dot_and_method"),  ❷
F.col("genres").getItem(0).alias("col_and_method"),
)

array_subset.show()

# +--------------+-------------+-------------+--------------+--------------+
# |          name|dot_and_index|col_and_index|dot_and_method|col_and_method|
# +--------------+-------------+-------------+--------------+--------------+
# |Silicon Valley|       Comedy|       Comedy|        Comedy|        Comedy|
# +--------------+-------------+-------------+--------------+--------------+


array_subset_repeated = array_subset.select(
    "name",
    F.lit("Comedy").alias("one"),
    F.lit("Horror").alias("two"),
    F.lit("Drama").alias("three"),
    F.col("dot_and_index"),
).select(
    "name",
    F.array("one", "two", "three").alias("Some_Genres"),                ❶
F.array_repeat("dot_and_index", 5).alias("Repeated_Genres"),        ❷
)

array_subset_repeated.show(1, False)

# +--------------+-----------------------+----------------------------------------+
# |name          |Some_Genres            |Repeated_Genres                         |
# +--------------+-----------------------+----------------------------------------+
# |Silicon Valley|[Comedy, Horror, Drama]|[Comedy, Comedy, Comedy, Comedy, Comedy]|
# +--------------+-----------------------+----------------------------------------+

array_subset_repeated.select(
    "name", F.size("Some_Genres"), F.size("Repeated_Genres")            ❸
).show()

# +--------------+-----------------+---------------------+
# |          name|size(Some_Genres)|size(Repeated_Genres)|
# +--------------+-----------------+---------------------+
# |Silicon Valley|                3|                    5|
# +--------------+-----------------+---------------------+
array_subset_repeated.select(
    "name",
    F.array_distinct("Some_Genres"),                                    ❹
F.array_distinct("Repeated_Genres"),                                ❹
).show(1, False)

# +--------------+---------------------------+-------------------------------+
# |name          |array_distinct(Some_Genres)|array_distinct(Repeated_Genres)|
# +--------------+---------------------------+-------------------------------+
# |Silicon Valley|[Comedy, Horror, Drama]    |[Comedy]                       |
# +--------------+---------------------------+-------------------------------+

array_subset_repeated = array_subset_repeated.select(
    "name",
    F.array_intersect("Some_Genres", "Repeated_Genres").alias(          ❺
"Genres"
),
)

array_subset_repeated.show()

# +--------------+--------+
# |          name|  Genres|
# +--------------+--------+
# |Silicon Valley|[Comedy]|
# +--------------+--------+

array_subset_repeated.select(
    "Genres", F.array_position("Genres", "Comedy")
).show()

# +--------+------------------------------+
# |  Genres|array_position(Genres, Comedy)|
# +--------+------------------------------+
# |[Comedy]|                             1|
# +--------+------------------------------+


columns = ["name", "language", "type"]

shows_map = shows.select(
    *[F.lit(column) for column in columns],
    F.array(*columns).alias("values"),
)

shows_map = shows_map.select(F.array(*columns).alias("keys"), "values")

shows_map.show(1)
# +--------------------+--------------------+
# |                keys|              values|
# +--------------------+--------------------+
# |[name, language, ...|[Silicon Valley, ...|
# +--------------------+--------------------+

shows_map = shows_map.select(
    F.map_from_arrays("keys", "values").alias("mapped")
)

shows_map.printSchema()

# root
#  |-- mapped: map (nullable = false)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)
shows_map.show(1, False)

# +---------------------------------------------------------------+
# |mapped                                                         |
# +---------------------------------------------------------------+
# |[name -> Silicon Valley, language -> English, type -> Scripted]|
# +---------------------------------------------------------------+

shows_map.select(
    F.col("mapped.name"),      ❶
F.col("mapped")["name"],   ❷
shows_map.mapped["name"],  ❸
).show()

# +--------------+--------------+--------------+
# |       name   |  mapped[name]|  mapped[name]|
# +--------------+--------------+--------------+
# |Silicon Valley|Silicon Valley|Silicon Valley|
# +--------------+--------------+--------------+

# Reducing duplicate data with complex data types

# use explode and collect operations to go from hierarchical to tabular and back.

# Explode breaks array of value into discreete records
episodes = shows.select(
    "id", F.explode("_embedded.episodes").alias("episodes")
)                                                              ❶
episodes.show(5, truncate=70)

# +---+----------------------------------------------------------------------+
# | id|                                                              episodes|
# +---+----------------------------------------------------------------------+
# |143|{{{http:/ /api.tvmaze.com/episodes/10897}}, 2014-04-06, 2014-04-07T0...|
# |143|{{{http:/ /api.tvmaze.com/episodes/10898}}, 2014-04-13, 2014-04-14T0...|
# |143|{{{http:/ /api.tvmaze.com/episodes/10899}}, 2014-04-20, 2014-04-21T0...|
# |143|{{{http:/ /api.tvmaze.com/episodes/10900}}, 2014-04-27, 2014-04-28T0...|
# |143|{{{http:/ /api.tvmaze.com/episodes/10901}}, 2014-05-04, 2014-05-05T0...|
# +---+----------------------------------------------------------------------+
# only showing top 5 rows

episodes.count()  # 53

'''
Explode can also happen with maps: the keys and values will be exploded in two different fields. 
For completeness, I’ll introduce the second type of explosion: posexplode(). 
The “pos” stands for position: it explodes the column and returns an additional column before the 
data that contains the position as a long.
'''

episode_name_id = shows.select(
    F.map_from_arrays(                                         ❶
F.col("_embedded.episodes.id"), F.col("_embedded.episodes.name")
).alias("name_id")
)

episode_name_id = episode_name_id.select(
    F.posexplode("name_id").alias("position", "id", "name")    ❷
)

episode_name_id.show(5)

# +--------+-----+--------------------+
# |position|   id|                name|
# +--------+-----+--------------------+
# |       0|10897|Minimum Viable Pr...|
# |       1|10898|       The Cap Table|
# |       2|10899|Articles of Incor...|
# |       3|10900|    Fiduciary Duties|
# |       4|10901|      Signaling Risk|
# +--------+-----+--------------------+
# only showing top 5 rows


''' For collecting the records into a complex column we use COLLECT_LIST() and COLLECT_SET()
collect_list() returns one array element per column record, 
collect_set() will return one array element per distinct column record, just like a Python set.
'''

collected = episodes.groupby("id").agg(
    F.collect_list("episodes").alias("episodes")
)

# Building your own hierarchies: Struct as a function
'''
To create a struct, we use the struct() function from the pyspark.sql.functions module. This function takes a number of columns as parameters (just like select()) and returns a struct column containing the columns passed as parameters as fields.
'''
struct_ex = shows.select(
    F.struct(                                          ❶
F.col("status"), F.col("weight"), F.lit(True).alias("has_watched")
).alias("info")

# =================================================================================================================================================================

# 7 Blending Python and SQL code

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException     ❶
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

elements = spark.read.csv(
    "./data/elements/Periodic_Table_Of_Elements.csv",
    header=True,
    inferSchema=True,
)

elements.where(F.col("phase") == "liq").groupby("period").count().show()
# In SQL: We assume that the data is in a table called `elements`

# SELECT
#   period,
#   count(*)
# FROM elements
# WHERE phase = 'liq'
# GROUP BY period;

try:
    spark.sql(
        "select period, count(*) from elements "
        "where phase='liq' group by period"
    ).show(5)
except AnalysisException as e:
    print(e)

# 'Table or view not found: elements; line 1 pos 29'

# When you want to create a table/view to query with Spark SQL, use the createOrReplaceTempView() method.
elements.createOrReplaceTempView("elements")  ❶

spark.sql(
    "select period, count(*) from elements where phase='liq' group by period"
).show(5)

# +------+--------+
# |period|count(1)|
# +------+--------+
# |     6|       1|
# |     4|       1|
# +------+--------+


'''
PySpark has four methods to create temporary views, and they look quite similar at first glance:

createGlobalTempView()

createOrReplaceGlobalTempView()

createOrReplaceTempView()

createTempView()
'''

DATA_DIRECTORY = "./data/backblaze/"

q1 = spark.read.csv(
    DATA_DIRECTORY + "drive_stats_2019_Q1", header=True, inferSchema=True
)
q2 = spark.read.csv(
    DATA_DIRECTORY + "data_Q2_2019", header=True, inferSchema=True
)
q3 = spark.read.csv(
    DATA_DIRECTORY + "data_Q3_2019", header=True, inferSchema=True
)
q4 = spark.read.csv(
    DATA_DIRECTORY + "data_Q4_2019", header=True, inferSchema=True
)

# Q4 has two more fields than the rest

q4_fields_extra = set(q4.columns) - set(q1.columns)

for i in q4_fields_extra:
    q1 = q1.withColumn(i, F.lit(None).cast(T.StringType()))
    q2 = q2.withColumn(i, F.lit(None).cast(T.StringType()))
    q3 = q3.withColumn(i, F.lit(None).cast(T.StringType()))

# if you are only using the minimal set of data, use this version
backblaze_2019 = q3

# if you are using the full set of data, use this version
backblaze_2019 = (
    q1.select(q4.columns)
    .union(q2.select(q4.columns))
    .union(q3.select(q4.columns))
    .union(q4)
)

# Setting the layout for each column according to the schema

backblaze_2019 = backblaze_2019.select(
    [
        F.col(x).cast(T.LongType()) if x.startswith("smart") else F.col(x)
        for x in backblaze_2019.columns
    ]
)

backblaze_2019.createOrReplaceTempView("backblaze_stats_2019")

spark.sql(
    "select serial_number from backblaze_stats_2019 where failure = 1"
).show(
    5
)          ❶

backblaze_2019.where("failure = 1").select(F.col("serial_number")).show(5)

# +-------------+
# |serial_number|
# +-------------+
# |    57GGPD9NT|
# |     ZJV02GJM|
# |     ZJV03Y00|
# |     ZDEB33GK|
# |     Z302T6CW|
# +-------------+
# only showing top 5 rows


# Grouping similar records together: group by and order by
spark.sql(
    """SELECT
           model,
           min(capacity_bytes / pow(1024, 3)) min_GB,
           max(capacity_bytes/ pow(1024, 3)) max_GB
        FROM backblaze_stats_2019
        GROUP BY 1
        ORDER BY 3 DESC"""
).show(5)

backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).orderBy(F.col("max_GB"), ascending=False).show(5)

# +--------------------+--------------------+-------+
# |               model|              min_GB| max_GB|
# +--------------------+--------------------+-------+
# |       ST16000NM001G|             14902.0|14902.0|
# | TOSHIBA MG07ACA14TA|-9.31322574615478...|13039.0|
# |HGST HUH721212ALE600|             11176.0|11176.0|
# |       ST12000NM0007|-9.31322574615478...|11176.0|
# |       ST12000NM0008|             11176.0|11176.0|
# +--------------------+--------------------+-------+
# only showing top 5 rows


# Filtering after grouping using having
spark.sql(
    """SELECT
           model,
           min(capacity_bytes / pow(1024, 3)) min_GB,
           max(capacity_bytes/ pow(1024, 3)) max_GB
        FROM backblaze_stats_2019
        GROUP BY 1
        HAVING min_GB != max_GB
        ORDER BY 3 DESC"""
).show(5)

backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).where(F.col("min_GB") != F.col("max_GB")).orderBy(
    F.col("max_GB"), ascending=False
).show( 5)

# +--------------------+--------------------+-------+
# |               model|              min_GB| max_GB|
# +--------------------+--------------------+-------+
# | TOSHIBA MG07ACA14TA|-9.31322574615478...|13039.0|
# |       ST12000NM0007|-9.31322574615478...|11176.0|
# |HGST HUH721212ALN604|-9.31322574615478...|11176.0|
# |       ST10000NM0086|-9.31322574615478...| 9314.0|
# |HGST HUH721010ALE600|-9.31322574615478...| 9314.0|
# +--------------------+--------------------+-------+
# only showing top 5 rows


# Creating new tables/views using the CREATE keyword
backblaze_2019.createOrReplaceTempView("drive_stats")

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW drive_days AS
        SELECT model, count(*) AS drive_days
        FROM drive_stats
        GROUP BY model"""
)

spark.sql(
    """CREATE OR REPLACE TEMP VIEW failures AS
           SELECT model, count(*) AS failures
           FROM drive_stats
           WHERE failure = 1
           GROUP BY model"""
)

drive_days = backblaze_2019.groupby(F.col("model")).agg(
    F.count(F.col("*")).alias("drive_days")
)

failures = (
    backblaze_2019.where(F.col("failure") == 1)
    .groupby(F.col("model"))
    .agg(F.count(F.col("*")).alias("failures"))
)

# Creating tables from data in SQL
spark.sql("create table q1 as select * from csv.`./data/backblaze/drive_stats_2019_Q1`")

# Adding data to our table using UNION and JOIN
columns_backblaze = ", ".join(q4.columns)   ❶

q1.createOrReplaceTempView("Q1")            ❷
q2.createOrReplaceTempView("Q2")
q3.createOrReplaceTempView("Q3")
q4.createOrReplaceTempView("Q4")

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW backblaze_2019 AS
    SELECT {col} FROM Q1 UNION ALL
    SELECT {col} FROM Q2 UNION ALL
    SELECT {col} FROM Q3 UNION ALL
    SELECT {col} FROM Q4
""".format(
        col=columns_backblaze
    )
)

backblaze_2019 = (❸
                  q1.select(q4.columns)
.union(q2.select(q4.columns))
.union(q3.select(q4.columns))
.union(q4)
)


spark.sql(
    """select
           drive_days.model,
           drive_days,
           failures
    from drive_days
    left join failures
    on
        drive_days.model = failures.model"""
).show(5)

drive_days.join(failures, on="model", how="left").show(5)

# Organizing your SQL code better through subqueries and common table expressions
spark.sql(
    """
    WITH drive_days as (             ❶
        SELECT                       ❶
            model,                   ❶
            count(*) AS drive_days   ❶
        FROM drive_stats             ❶
        GROUP BY model),             ❶
    failures as (                    ❶
        SELECT                       ❶
            model,                   ❶
            count(*) AS failures     ❶
        FROM drive_stats             ❶
        WHERE failure = 1            ❶
        GROUP BY model)              ❶
    SELECT
        failures.model,
        failures / drive_days failure_rate
    FROM drive_days
    INNER JOIN failures
    ON
        drive_days.model = failures.model
    ORDER BY 2 desc
    """
).show(5)

'''
In Python, the best alternative I’ve found is to wrap statements in a function. 
Any intermediate variable created in the scope of the function would not be kept once the function returns. 
My version of the query using PySpark is in the next listing.
'''


def failure_rate(drive_stats):
    drive_days = drive_stats.groupby(F.col("model")).agg(     ❶
    F.count(F.col("*")).alias("drive_days")
    )
    failures = (
        drive_stats.where(F.col("failure") == 1)
        .groupby(F.col("model"))
        .agg(F.count(F.col("*")).alias("failures"))
    )
    answer = (❷
              drive_days.join(failures, on="model", how="inner")
    .withColumn("failure_rate", F.col("failures") / F.col("drive_days"))
    .orderBy(F.col("failure_rate").desc())
    )
    return answer


failure_rate(backblaze_2019).show(5)

print("drive_days" in dir())

# Blending SQL and Python

from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

DATA_DIRECTORY = "./data/backblaze/"

DATA_FILES = [
    "drive_stats_2019_Q1",
    "data_Q2_2019",
    "data_Q3_2019",
    "data_Q4_2019",
]

data = [
    spark.read.csv(DATA_DIRECTORY + file, header=True, inferSchema=True)
    for file in DATA_FILES
]

common_columns = list(
    reduce(lambda x, y: x.intersection(y), [set(df.columns) for df in data])
)

assert set(["model", "capacity_bytes", "date", "failure"]).issubset(
    set(common_columns)
)

full_data = reduce(
    lambda x, y: x.select(common_columns).union(y.select(common_columns)), data
)

full_data = full_data.selectExpr(
    "model", "capacity_bytes / pow(1024, 3) capacity_GB", "date", "failure"
)

drive_days = full_data.groupby("model", "capacity_GB").agg(
    F.count("*").alias("drive_days")
)

failures = (
    full_data.where("failure = 1")
    .groupby("model", "capacity_GB")
    .agg(F.count("*").alias("failures"))
)

summarized_data = (
    drive_days.join(failures, on=["model", "capacity_GB"], how="left")
    .fillna(0.0, ["failures"])
    .selectExpr("model", "capacity_GB", "failures / drive_days failure_rate")
    .cache()
)
# =========================================================================================================================================================================
# 8 Extending PySpark with Python: RDD and UDFs
# promote it to an RDD via the parallelize method
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

collection = [1, "two", 3.0, ("four", 4), {"five": 5}]             ❶

sc = spark.sparkContext                                            ❷

collection_rdd = sc.parallelize(collection)                        ❸

print(collection_rdd)
# ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:195


# Manipulating data the RDD way: map(), filter(), and reduce()

# APPLY ONE FUNCTION TO EVERY OBJECT: MAP
from py4j.protocol import Py4JJavaError


def add_one(value):
    return value + 1                           ❶
    '''The above function when applied to each element of RDD will Fail as there are string and int in collection RDD '''

    collection_rdd = collection_rdd.map(add_one)   ❷

    try:
        print(collection_rdd.collect())            ❸
        except Py4JJavaError:
        pass
    ❶ A
    seemingly
    inoffensive
    function, add_one()
    adds
    1
    to
    the
    value
    passed as an
    argument.

    ❷ I
    apply
    my
    function
    to
    every
    element in the
    RDD
    via
    the
    map()
    method.

    ❸ collect()
    materializes
    an
    RDD
    into
    a
    Python
    list
    on
    the
    master
    node.

    collection_rdd = sc.parallelize(collection)       ❶

    def safer_add_one(value):
        try:
            return value + 1
        except TypeError:
            return value                              ❷
            collection_rdd = collection_rdd.map(safer_add_one)

            print(collection_rdd.collect())
            # [2, 'two', 4.0, ('four', 4), {'five': 5}]       ❸

            # ONLY KEEP WHAT YOU WANT USING FILTER
            The
            RDD
            version
            of
            filter() is a
            little
            different
            than
            the
            data
            frame
            version: it
            takes
            a
            function
            f, which
            applies
            to
            each
            object( or element) and keeps
            only
            those
            that
            return a
            truthful
            value

            collection_rdd = collection_rdd.filter(lambda elem: isinstance(elem, (float, int)))

            # TWO ELEMENTS COME IN, ONE COMES OUT: REDUCE()
            from operator import add                ❶

            collection_rdd = sc.parallelize([4, 7, 9, 1, 3])

            print(collection_rdd.reduce(add))  # 24

            # Using Python to extend PySpark via UDFs
            import pyspark.sql.functions as F
            import pyspark.sql.types as T

            fractions = [[x, y] for x in range(100) for y in range(1, 100)]    ❶

            frac_df = spark.createDataFrame(fractions, ["numerator", "denominator"])

            frac_df = frac_df.select(
                F.array(F.col("numerator"), F.col("denominator")).alias(       ❷
            "fraction"
            ),
            )

            frac_df.show(5, False)
            # +--------+
            # |fraction|
            # +--------+
            # |[0, 1]  |
            # |[0, 2]  |
            # |[0, 3]  |
            # |[0, 4]  |
            # |[0, 5]  |
            # +--------+
            # only showing top 5 rows
            ❶ I
            start
            the
            range
            for the denominator at 1, since a fraction with 0 for the denominator is undefined.

            ❷ The
            array()
            function
            takes
            two or more
            columns
            of
            the
            same
            type and creates
            a
            single
            column
            containing
            an
            array
            of
            the
            columns
            passed as a
            parameter.

            from fractions import Fraction                                     ❶
            from typing import Tuple, Optional                                 ❷

            Frac = Tuple[int, int]                                             ❸

            def py_reduce_fraction(frac: Frac) -> Optional[Frac]:
                ❹

            """Reduce a fraction represented as a 2-tuple of integers."""
            num, denom = frac
            if denom:
                answer = Fraction(num, denom)
                return answer.numerator, answer.denominator
            return None
        assert py_reduce_fraction((3, 6)) == (1, 2)                        ❺
        assert py_reduce_fraction((1, 0)) is None

        def py_fraction_to_float(frac: Frac) -> Optional[float]:
            """Transforms a fraction represented as a 2-tuple of integers into a float."""
            num, denom = frac
            if denom:
                return num / denom
            return None

        assert py_fraction_to_float((2, 8)) == 0.25
        assert py_fraction_to_float((10, 0)) is None

        # PySpark provides a udf() function in the pyspark.sql.functions module to promote Python functions to their UDF equivalents
        SparkFrac = T.ArrayType(T.LongType())                        ❶

        reduce_fraction = F.udf(py_reduce_fraction, SparkFrac)       ❷

        frac_df = frac_df.withColumn(
            "reduced_fraction", reduce_fraction(F.col("fraction"))   ❸
        )

        frac_df.show(5, False)

        # +--------+----------------+
        # |fraction|reduced_fraction|
        # +--------+----------------+
        # |[0, 1]  |[0, 1]          |
        # |[0, 2]  |[0, 1]          |
        # |[0, 3]  |[0, 1]          |
        # |[0, 4]  |[0, 1]          |
        # |[0, 5]  |[0, 1]          |
        # +--------+----------------+
        # only showing top 5 rows

        # You also have the option of creating your Python function and promoting it as a UDF using the udf function as a decorator. Decorators are functions applied to other functions through the @ sign above the function definition

        @F.udf(T.DoubleType())

        ❶

        def fraction_to_float(frac: Frac) -> Optional[float]:
            """Transforms a fraction represented as a 2-tuple of integers into a float."""
            num, denom = frac
            if denom:
                return num / denom
            return None

        frac_df = frac_df.withColumn(
            "fraction_float", fraction_to_float(F.col("reduced_fraction"))
        )

        frac_df.select("reduced_fraction", "fraction_float").distinct().show(
            5, False
        )
        # +----------------+-------------------+
        # |reduced_fraction|fraction_float     |
        # +----------------+-------------------+
        # |[3, 50]         |0.06               |
        # |[3, 67]         |0.04477611940298507|
        # |[7, 76]         |0.09210526315789473|
        # |[9, 23]         |0.391304347826087  |
        # |[9, 25]         |0.36               |
        # +----------------+-------------------+
        # only showing top 5 rows
        assert fraction_to_float.func((1, 2)) == 0.5            ❷

        # ===============================================================================================================================================================

        # Big data is just a lot of small data: Using pandas UDFs
        # Column transformations with pandas: Using Series UDF
        gsod = (
            reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                [
                    spark.read.parquet(f"./data/gsod_noaa/gsod{year}.parquet")
                    for year in range(2010, 2021)
                ],
            )
            .dropna(subset=["year", "mo", "da", "temp"])
            .where(F.col("temp") != 9999.9)
            .drop("date")
        )

        # Connecting Spark with BigQuery
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.19.1", ❶
        ).getOrCreate()

        # READING THE DATA FROM BIGQUERY USING OUR SECRET KEY
        # Modifing the above script

        from functools import reduce
        import pyspark.sql.functions as F

        def read_df_from_bq(year):
            ❶

        return (
            spark.read.format("bigquery").option(                      ❷
        "table", f"bigquery-public-data.noaa_gsod.gsod{year}"  ❸
        )
        # .option("credentialsFile", "bq-key.json")                ❹
        .load()
        )


        gsod = (
            reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                [read_df_from_bq(year) for year in range(2010, 2021)],     ❺
        )
        .dropna(subset=["year", "mo", "da", "temp"])
        .where(F.col("temp") != 9999.9)
        .drop("date")
        )

        # Using for loop to achieve the above but try using reduce for better results
        gsod_alt = read_df_from_bq(2010)     ❶
        for year in range(2011, 2020):
            gsod_alt = gsod_alt.unionByName(
                read_df_from_bq(year), allowMissingColumns=True
            )
        gsod_alt = gsod_alt.drop("date")

        # Using Pandas UDF
        import pandas as pd
        import pyspark.sql.types as T

        @F.pandas_udf(T.DoubleType())

        ❶

        def f_to_c(degrees: pd.Series) -> pd.Series:
            ❷

        """Transforms Farhenheit to Celsius."""
        return (degrees - 32) * 5 / 9

        gsod = gsod.withColumn("temp_c", f_to_c(F.col("temp")))
        gsod.select("temp", "temp_c").distinct().show(5)

        from typing import Tuple

        @F.pandas_udf(T.DateType())
        def create_date(
                year_mo_da: Iterator[Tuple[pd.Series, pd.Series, pd.Series]]
        ) -> Iterator[pd.Series]:
            """Merges three cols (representing Y-M-D of a date) into a Date col."""
            for year, mo, da in year_mo_da:
                yield pd.to_datetime(
                    pd.DataFrame(dict(year=year, month=mo, day=da))
                )

        gsod.select(
            "year", "mo", "da",
            create_date(F.col("year"), F.col("mo"), F.col("da")).alias("date"),
        ).distinct().show(5)

        # ======================================================================================================================================================================

        # Window functions
        gsod = spark.read.parquet("./data/window/gsod.parquet")
        from pyspark.sql.window import Window
        each_year = Window.partitionBy("year")

        (gsod
         .withColumn("min_temp", F.min("temp").over(each_year))
         .where("temp = min_temp")
         .select("year", "mo", "da", "stn", "temp")
         .orderBy("year", "mo", "da")
         .show())
        # +----+---+---+------+------+
        # |year| mo| da|   stn|  temp|
        # +----+---+---+------+------+
        # |2017| 06| 20|896250|-114.7|
        # |2018| 08| 27|896060|-113.5|
        # |2019| 06| 15|895770|-114.7|
        # +----+---+---+------+------+

        gsod.select(
            "year",
            "mo",
            "da",
            "stn",
            "temp",
            F.min("temp").over(each_year).alias("min_temp"),
        ).where(
            "temp = min_temp"
        ).drop(                 ❶
        "min_temp"
        ).orderBy(
            "year", "mo", "da"
        ).show()

        temp_per_month_asc = Window.partitionBy("mo").orderBy("count_temp")
        gsod_light.withColumn(
            "rank_tpm", F.rank().over(temp_per_month_asc)    ❶
        ).show()

        gsod_light.withColumn(
            "rank_tpm", F.dense_rank().over(temp_per_month_asc)   ❶
        ).show()

        temp_each_year = each_year.orderBy("temp")                    ❶

        gsod_light.withColumn(
            "rank_tpm", F.percent_rank().over(temp_each_year)
        ).show()

        gsod_light.withColumn("rank_tpm", F.ntile(2).over(temp_each_year)).show()

        gsod_light.withColumn(
            "rank_tpm", F.row_number().over(temp_each_year)
        ).show()

        temp_per_month_desc = Window.partitionBy("mo").orderBy(
            F.col("count_temp").desc()                           ❶
        )

        gsod_light.withColumn(
            "row_number", F.row_number().over(temp_per_month_desc)
        ).show()

        gsod_light.withColumn(
            "previous_temp", F.lag("temp").over(temp_each_year)
        ).withColumn(
            "previous_temp_2", F.lag("temp", 2).over(temp_each_year)
        ).show()

        @F.pandas_udf("double")
        def median(vals: pd.Series) -> float:
            return vals.median()

        gsod_light.withColumn(
            "median_temp", median("temp").over(Window.partitionBy("year"))    ❶
        ).withColumn(
            "median_temp_g",
            median("temp").over(
                Window.partitionBy("year").orderBy("mo", "da")                ❷
        ), ❷
        ).show()