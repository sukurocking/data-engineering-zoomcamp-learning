# Objective is to get the data from a url, clean the data and then put into a GCP bucket (in parquet format)
# Then we finally put the data in the data warehouse (BigQuery)

import os
# from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
# from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# gcp_credentials_block = GcpCredentials.load("gcp-zoom-credentials")




@flow(retries=3, log_prints=True)
def main_flow(color: str, month: str, year: str):
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"
    
    # Extract the data from the url
    df = extract_data(url, color, year, month)
    # print(df.dtypes)
    # print(df.head())
    
    # Clean the dataframe
    clean_df = clean_data(df)
    
    
    # Writing the cleaned df to a parquet file in local filesystem
    local_path = f"../data_files/{color}/raw/clean_{color}_tripdata_{year}-{month}.csv.gz"
    # clean_df.to_parquet(path=local_path)
    clean_df.to_csv(local_path, compression="gzip")
    
    # Writing the parquet file to GCS bucket
    gcp_cloud_storage_bucket = GcsBucket.load("gcs-bucket-zoom-new")
    gcp_path =  f"{color}/{color}_tripdata_{year}-{month}.csv.gz"
    
    gcp_cloud_storage_bucket.upload_from_path(from_path = local_path, to_path = gcp_path)
    
    # Deleting local files
    delete_files()
    
    
    

# @task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
@task(retries=2, log_prints=True)
def extract_data(url: str, color: str, year: str, month: str) -> pd.DataFrame:
    download_path = f"../data_files/{color}/raw/{color}_tripdata_{year}-{month}.csv.gz"
    os.system(f"wget -O {download_path} {url}")
    raw_df = pd.read_csv(download_path)
    if color == "yellow":
        datetime_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    else:
        datetime_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
    for col in datetime_cols:
        raw_df[col] = pd.to_datetime(raw_df[col])
    return raw_df


@task(retries=3, log_prints=True)
def clean_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    print(f'pre: missing passenger count: {raw_df["passenger_count"].isna().sum()}')
    # raw_df["passenger_count"].fillna(0, inplace=True)
    raw_df = raw_df[raw_df["passenger_count"].isna()==False]
    print(f'post: missing passenger count: {raw_df["passenger_count"].isna().sum()}')
    return raw_df

# @task(retries=3, log_prints=True)
# def extract_data_to_gcs(from_path: str, to_path: str) -> None:
#     pass


@task(retries=3, log_prints=True)
def delete_files() -> None:
    raw_file = f"../data_files/{color}/raw/{color}_tripdata_{year}-{month}.csv.gz"
    cleaned_file = f"../data_files/{color}/raw/clean_{color}_tripdata_{year}-{month}.csv.gz"
    for file in [raw_file, cleaned_file]:
        os.unlink(file)

if __name__ == "__main__":
    # month = "02"
    # year = "2019"
    color = "green"
    months_list = range(1,13)
    months_list_str = [f"{month:02}" for month in months_list]
    for year in ["2019"]:
        for month in months_list_str:
            main_flow(color, month, year)
