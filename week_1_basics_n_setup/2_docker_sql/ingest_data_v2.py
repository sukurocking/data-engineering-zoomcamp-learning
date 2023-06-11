#!/usr/bin/env python
# coding: utf-8

import sqlalchemy
import pandas as pd
from time import time
# import argparse
import os
from prefect import flow, task
from datetime import timedelta
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="Extract Data", log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    data_files_path = "../../data_files"
    if not os.path.exists(data_files_path):
        os.mkdir(data_files_path)
    download_file_path = data_files_path + "/data_file.csv.gz"
    
    os.system(f"wget -q -O {download_file_path} {url}")
    df_iter = pd.read_csv(download_file_path, iterator=True, chunksize=100000)
    df = next(df_iter)

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    
    return df
    

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing count: {df[df['passenger_count'].isin([0])]['passenger_count'].count()}") 
    df = df[df["passenger_count"] > 0]
    print(f"post: missing count: {df[df['passenger_count'].isin([0])]['passenger_count'].count()}") 
    
    return df
    

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    connectionblock = SqlAlchemyConnector.load("sqlalchemyblock")
    with connectionblock.get_connection(begin=False) as engine:
        # Create the structure of the table
        df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

        # Uploading the first chunk into postgres database
        df.to_sql(name=table_name,       #table_name \
                con=engine,                 #connection name\
                if_exists="append")      #append data

        t_start = time()    
        df.to_sql(name="yellow_taxi_data", #table_name \
                con=engine,                 #connection name\
                if_exists="append")       #append data

        t_end = time()
        print("insertion completed successfully, took %.3f seconds" % (t_end - t_start))
        
@flow(name="Subflow", log_prints=True)
def log_subflow(table_name : str):
    print(f"Logging flow for {table_name}")

    
@flow(log_prints=True, retries=3)
def main_flow(table_name : str):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-02.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)
    

if __name__ == "__main__":
    main_flow(table_name = "yellow_taxi_data")
        
