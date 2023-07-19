#!/usr/bin/env python

# Steps of a sample mini-project on creating a report on green and yellow taxi data
# 1. Reading data from pq folder (both yellow and green taxi data)
# 2. Picking the common columns from both yellow and green taxi data
# 3. Merging the yellow and green taxi data
# 4. Summarizing the yellow and green taxi data with no. of records, sum trip amount


from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.master("local[*]").appName('testapp').getOrCreate()

colors = ["green", "yellow"]

# The below is a dictionary consisting of the dataframes
df_dict = dict()
for color in colors:
    df_dict[color] = spark.read.option("header", "true").\
                        option("inferschema", "true").\
                        format("csv").\
                        load(f"../../data_files/{color}/raw/*")


for color in colors:
    df_dict[color] = df_dict[color].withColumn("servicetype", f.lit(color))


print(f"Dictionary keys: f{df_dict.keys()}")

print(df_dict["green"].printSchema())

print(df_dict["yellow"].printSchema())

df_dict["green"] = df_dict["green"].withColumnsRenamed(
    {"lpep_pickup_datetime": "pickup_datetime",
     "lpep_dropoff_datetime": "dropoff_datetime"}
)

df_dict["yellow"] = df_dict["yellow"].withColumnsRenamed(
    {"tpep_pickup_datetime": "pickup_datetime",
     "tpep_dropoff_datetime": "dropoff_datetime"}
)

print(f"#Number of green columns: {len(df_dict['green'].columns)} ")
print(f"#Number of yellow columns: {len(df_dict['yellow'].columns)} ")


common_columns = []
for col in df_dict["yellow"].columns:
    if col in df_dict["green"].columns:
        common_columns.append(col)

print(common_columns)
print(len(common_columns))


df_dict_to_merge = {}  # Initializing a dictionary of the merged dataframes
# This below step merges the yellow and green taxi dataframes
for color in ["green", "yellow"]:
    df_dict_to_merge[color] = df_dict[color].select(common_columns)


df_dict_to_merge["yellow"].show(5)

# Merging the dataframes
df_final = df_dict_to_merge["green"].unionAll(df_dict_to_merge["yellow"])
df_final.printSchema()

df_final.select("servicetype").distinct().show()

# Group by servicetype to get the summary of no of records and amount column
