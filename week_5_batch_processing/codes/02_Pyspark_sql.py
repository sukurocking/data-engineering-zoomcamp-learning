#!/usr/bin/env python
# coding: utf-8


### Steps of a sample mini-project on creating a report on green and yellow taxi data
# 1. Reading data from pq folder (both yellow and green taxi data)
# 2. Picking the common columns from both yellow and green taxi data
# 3. Merging the yellow and green taxi data
# 4. Summarizing the yellow and green taxi data with no. of records, sum trip amount


# Below step reads the csv.gz file and converts to parquet format
# This may not be required in the final code 
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder \
            .appName('testapp') \
            .getOrCreate()



colors = ["green", "yellow"]


# The below is a dictionary consisting of the dataframes
df_dict = dict()
for color in colors:
    df_dict[color] = spark.read.option("header", "true") \
                    .option("inferschema", "true") \
                    .format("csv") \
                    .load(f"../../data_files/{color}/raw/*")


for color in colors:
    df_dict[color] = df_dict[color].withColumn("servicetype", f.lit(color))


# Renaming the pickup and dropoff columns
df_dict["green"] = df_dict["green"].withColumnsRenamed(
                                    {"lpep_pickup_datetime":"pickup_datetime", 
                                     "lpep_dropoff_datetime" : "dropoff_datetime"}
                                    )
df_dict["yellow"] = df_dict["yellow"].withColumnsRenamed(
                                    {"tpep_pickup_datetime":"pickup_datetime", 
                                     "tpep_dropoff_datetime" : "dropoff_datetime"}
                                    )

# Specifying list of columns
common_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'RatecodeID',
    'store_and_fwd_flag',
    'PULocationID',
    'DOLocationID',
    'payment_type',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'congestion_surcharge',
    'servicetype'
]


print(len(common_columns))

#Initializing a dictionary of the merged dataframes
df_dict_to_merge = {} 

#This below step selects the common columns between yellow and green merges the yellow and green taxi dataframes
for color in ["green", "yellow"]:
    df_dict_to_merge[color] = df_dict[color].select(common_columns)


df_final = df_dict_to_merge["green"].unionAll(df_dict_to_merge["yellow"])

df_final.createOrReplaceTempView("df_sql_view")

# Group by servicetype to get the summary of amount columns
df_summary = spark.sql("""
    select 
    -- Grouping
    servicetype,
    DATE_TRUNC("month", pickup_datetime) as pickup_month,
    PULocationID as zone,
    
    -- Revenue columns
    sum(total_amount) as revenue_monthly_total_amt,
    sum(tip_amount) as revenue_monthly_tip_amt,
    sum(fare_amount) as revenue_monthly_fare_amt
    from df_sql_view
    group by 1, 2, 3
    """
    )


df_summary.write.mode("overwrite"). \
    parquet("../../data_files/report/report-2019/")
