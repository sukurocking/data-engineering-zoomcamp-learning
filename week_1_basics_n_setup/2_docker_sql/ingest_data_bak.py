#!/usr/bin/env python
# coding: utf-8

import sqlalchemy
import pandas as pd
from time import time
import argparse
import os



parser = argparse.ArgumentParser(description='Builds a pipeline to download data from a url and ingest data into a postgres table')
parser.add_argument('--user', type=str, required=True, help='POSTGRES_USER')
parser.add_argument('--password', type=str, required=True, help='POSTGRES_PASSWORD')
parser.add_argument('--host', type=str, required=True, help='POSTGRES_HOST')
parser.add_argument('--port', type=str, required=True, help='POSTGRES PORT')
parser.add_argument('--dbname', type=str, required=True, help='POSTGRES_DB')
parser.add_argument('--tablename', type=str, required=True, help='POSTGRES_TABLENAME')
# parser.add_argument('--yrmon', type=str, required=True, help="TARGET_MONTH_DATA_TO_INGEST in format YYYY-MM")
parser.add_argument('--url', type=str, required=True, help='URL path from where the data is saved and is to be ingested')

args = parser.parse_args()
# print(args.user, args.password, args.dbname, args.tablename)

host = args.host
port = args.port
pg_url = f'postgresql://{args.user}:{args.password}@{host}:{port}/{args.dbname}'
con = sqlalchemy.create_engine(pg_url, client_encoding='utf8')
# exit()

# url to download from 
url = args.url
data_files_path = "./data_files"
if not os.path.exists(data_files_path):
    os.mkdir(data_files_path)

download_file_path = data_files_path + "/data_file.csv.gz"

# Download the gz file and unzip to csv file
os.system(f"wget -q -O {download_file_path} " + url)
os.system(f"gunzip {download_file_path}")

# exit()

# This gives the schema of the database specific to postgres
# print(pd.io.sql.get_schema(df, "yellow_taxi_data", con=con))


# Now, we want to create an empty table in postgres with the structure as specified above

# df.to_sql(name="yellow_taxi_data", con=con, if_exists="replace", chunksize=100000)
csv_file_path = data_files_path + f"/data_file.csv"
df_iter = pd.read_csv(csv_file_path, iterator=True, chunksize=1000000)
df = next(df_iter)

df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])


# Create the structure of the table
df.head(0).to_sql(name="yellow_taxi_data", con=con, if_exists="replace")

# Uploading the first chunk into postgres database
df.to_sql(name="yellow_taxi_data", #table_name \
        con=con,                 #connection name\
        if_exists="append")       #append data



while True:
    t_start = time()    
    df = next(df_iter)
    
    # Changing the datatype of date-time columns to datatime (since they are being read as TEXT)
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
                           
    df.to_sql(name="yellow_taxi_data", #table_name \
            con=con,                 #connection name\
            if_exists="append")       #append data

    t_end = time()
    
    print("insertion completed successfully, took %.3f seconds" % (t_end - t_start))

