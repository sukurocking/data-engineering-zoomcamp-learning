#!/usr/bin/env python
# coding: utf-8

import sqlalchemy
import pandas as pd
from time import time
# import argparse
import os


def ingest_data(user, password, host, port, db, table_name, csv_url):
    data_files_path = "./data_files"
    if not os.path.exists(data_files_path):
        os.mkdir(data_files_path)
    download_file_path = data_files_path + "/data_file.csv.gz"
    
    os.system(f"wget -q -O {download_file_path} {csv_url}")
    pg_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    con = sqlalchemy.create_engine(pg_url, client_encoding='utf8')
    df_iter = pd.read_csv(download_file_path, iterator=True, chunksize=1000000)
    df = next(df_iter)

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

        
    # Create the structure of the table
    df.head(0).to_sql(name=table_name, con=con, if_exists="replace")

    # Uploading the first chunk into postgres database
    df.to_sql(name=table_name,       #table_name \
            con=con,                 #connection name\
            if_exists="append")      #append data


    while True:
        try:
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
        
        except StopIteration:
            print("Finished ingesting data into the postgres database")    
            break
    


if __name__ == "__main__":
        user = "root"
        password = "root"
        host = "localhost"
        port = "5432"
        db = "ny_taxi"
        table_name = "yellow_taxi_data"
        csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-02.csv.gz"
        ingest_data(user, password, host, port, db, table_name, csv_url)
        
