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
def main_flow():
    month = "02"
    year = "2019"
    color = "yellow"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"
    
    # Extract the data from the url
    df = extract_data(url, color, year, month)
    # print(df.dtypes)
    # print(df.head())
    
    # Clean the dataframe
    clean_df = clean_data(df)
    
    
    # Writing the cleaned df to a parquet file in local filesystem
    local_path = f"../data_files/{color}/cleaned-{year}-{month}.csv.gz"
    # clean_df.to_parquet(path=local_path)
    clean_df.to_csv(local_path, compression="gzip")
    
    # Writing the parquet file to GCS bucket
    gcp_cloud_storage_bucket = GcsBucket.load("gcs-bucket-zoom-new")
    gcp_path =  f"{color}/{year}-{month}.csv.gz"
    
    gcp_cloud_storage_bucket.upload_from_path(from_path = local_path, to_path = gcp_path)
    
    

@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str, color: str, year: str, month: str) -> pd.DataFrame:
    download_path = f"../data_files/{color}/{year}-{month}.csv.gz"
    os.system(f"wget -O {download_path} {url}")
    raw_df = pd.read_csv(download_path)
    raw_df["tpep_pickup_datetime"] = pd.to_datetime(raw_df["tpep_pickup_datetime"])
    raw_df["tpep_pickup_datetime"] = pd.to_datetime(raw_df["tpep_pickup_datetime"])
    return raw_df


@task(retries=3, log_prints=True)
def clean_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    print(f'pre: missing passenger count: {raw_df["passenger_count"].isin([0]).sum()}')
    # raw_df["passenger_count"].fillna(0, inplace=True)
    raw_df = raw_df[raw_df["passenger_count"] > 0]
    print(f'post: missing passenger count: {raw_df["passenger_count"].isin([0]).sum()}')
    return raw_df

# @task(retries=3, log_prints=True)
# def extract_data_to_gcs(from_path: str, to_path: str) -> None:
#     pass


if __name__ == "__main__":
    main_flow()
