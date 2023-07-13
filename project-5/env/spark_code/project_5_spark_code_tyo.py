from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col, max


spark = SparkSession.builder \
    .appName("Project_5_Tyo") \
    .getOrCreate()

file = "/Users/fbramantyo/Downloads/fhv_tripdata_2021-02.parquet"
tempView = "fhv_tripdata"

df_data = spark.read.parquet(file)

df_data.createOrReplaceTempView(tempView)

# print(type(df_data))

sql1 = """
        SELECT 
            COUNT(*) AS count_trip_20210215 
        FROM {table} 
        WHERE CAST(pickup_datetime as date) = '2021-02-15'
    """.format(table = tempView)
sql2 = """
        SELECT 
            CAST(pickup_datetime as date) AS date,
            MAX(DATEDIFF(minute, pickup_datetime, dropOff_datetime)) AS longest_trip_duration_in_second
        FROM {table}
        GROUP BY CAST(pickup_datetime as date)
        ORDER BY CAST(pickup_datetime as date)
    """.format(table = tempView)
sql3 = """
    SELECT 
        dispatching_base_num,
        COUNT(*) AS count
    FROM {table} 
    GROUP BY dispatching_base_num
    ORDER BY count DESC
    LIMIT 5
""".format(table = tempView)
sql4 = """
    SELECT 
        PUlocationID, 
        DOlocationID,
        COUNT(*) AS count
    FROM {table} 
    WHERE PUlocationID IS NOT NULL AND DOlocationID IS NOT NULL
    GROUP BY PUlocationID, DOlocationID
    ORDER BY count DESC
    LIMIT 5
""".format(table = tempView)

#1
print("1. Taxy trip on February 15")
query = spark.sql(sql1)
query.show()

#2
print("2. Longest trip for each day")
query = spark.sql(sql2)
query.show(30)

#3
print("3. Top 5 most frequent `dispatching_base_num`")
query = spark.sql(sql3)
query.show()

#4
print("4. Top 5 most common location pairs (PUlocationID and DOlocationID)")
query = spark.sql(sql4)
query.show()
