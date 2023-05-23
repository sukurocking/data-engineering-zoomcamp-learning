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
parser.add_argument('--dbname', type=str, required=True, help='POSTGRES_DB')
parser.add_argument('--tablename', type=str, required=True, help='POSTGRES_TABLENAME')
parser.add_argument('--yrmon', type=str, required=True, help="TARGET_MONTH_DATA_TO_INGEST in format YYYY-MM")

args = parser.parse_args()
# print(args.user, args.password, args.dbname, args.tablename)

host = 'localhost'
port = "5432"
pg_url = f'postgresql://{args.user}:{args.password}@{host}:{port}/{args.dbname}'
con = sqlalchemy.create_engine(pg_url, client_encoding='utf8')
# exit()

# url to download from 
month = args.yrmon
url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{args.yrmon}.csv.gz"
data_files_path = "./data_files"
if not os.path.exists(data_files_path):
    os.mkdir(data_files_path)

download_file_path = data_files_path + f"/data_file_{args.yrmon}.csv.gz"

# Download the gz file and unzip to csv file
os.system(f"wget -O {download_file_path} " + url)
os.system(f"gunzip {download_file_path}")

# exit()

# This gives the schema of the database specific to postgres
# print(pd.io.sql.get_schema(df, "yellow_taxi_data", con=con))


# Now, we want to create an empty table in postgres with the structure as specified above

# df.to_sql(name="yellow_taxi_data", con=con, if_exists="replace", chunksize=100000)
csv_file_path = data_files_path + f"/data_file_{args.yrmon}.csv"
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

