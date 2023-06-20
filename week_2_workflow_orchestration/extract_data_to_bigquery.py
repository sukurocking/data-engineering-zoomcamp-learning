# Objective is to get the data from a url, clean the data and then put into a GCP bucket (in parquet format)
# Then we finally put the data in the data warehouse (BigQuery)

import os
# from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_cloud_storage


@flow(retries=3, log_prints=True)
def main_flow():
    month = "02"
    year = "2019"
    color = "yellow"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"

    # Extract the data from the url - Task #1
    df = extract_data(url)
    # print(df.dtypes)
    # print(df.head())

    # Clean the dataframe - Task #2
    clean_df = clean_data(df)

    # Writing the cleaned df to a parquet file in local filesystem
    local_path = f"../../data_files/{color}/{year}-{month}.parquet"
    clean_df.to_parquet(path=local_path)

    # Writing the parquet file to GCS bucket - Task #3
    gcp_path = f"{color}/{year}-{month}.parquet"
    extract_data_to_gcs(from_path=local_path, to_path=gcp_path)

    bucket_name = "dtc-data-lake-dtc-de-course-388001"
    datasetname = "trips_data_all"
    tablename = f"ny_{color}_taxi"
    uri = f"gs://{bucket_name}/{color}/{year}-{month}.parquet"
    upload_data_to_bq(datasetname,
                      tablename,
                      uri)


@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str) -> pd.DataFrame:
    download_path = "../../data_files/data_file.csv.gz"
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


@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data_to_gcs(from_path: str, to_path: str) -> None:
    gcp_cloud_storage_bucket = GcsBucket.load("gcs-bucket-zoom-new")
    gcp_cloud_storage_bucket.upload_from_path(from_path=from_path, to_path=to_path)


@flow(name="Subflow - Upload Data to BQ", log_prints=True)
def upload_data_to_bq(dataset: str, tablename: str, gsutil_uri: str):
    gcp_credentials_block = GcpCredentials.load("gcp-zoom-credentials")
    bigquery_load_cloud_storage(
        dataset=dataset,
        table=tablename,
        uri=gsutil_uri
    )


if __name__ == "__main__":
    main_flow()
