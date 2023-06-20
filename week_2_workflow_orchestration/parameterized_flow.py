import os

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

@flow()
def main_flow():
    color = "yellow"
    year = "2020"
    month = "02"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"
    local_path = f"../data_files/{color}/{color}_tripdata_{year}-{month}.csv.gz"
    clean_data_local_path = f"../data_files/{color}/clean_{color}_tripdata_{year}-{month}.csv.gz"
    download_from_web_to_local(url, local_path)
    clean_df = clean_data(local_path, clean_data_local_path)
    gs_path = ""
    web_to_gcs_flow(url, clean_data_local_path, gs_path)
    bq_dataset = "trips_data_all"
    bq_table = "ny_taxi"
    gcs_upload_to_bq(gs_path, bq_dataset, bq_table)

def web_to_gcs_flow(url: str, df_local_path: str, gs_path: str):
    # download_from_web_to_local(url)
    pass

# Download from Web to local
@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_from_web_to_local(url: str, local_path: str):
    # Downloads from a web url to local file
    os.system(f"wget -O {local_path} {url}")

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def clean_data(local_path: str, clean_data_local_path: str) -> pd.DataFrame:
    """
    Corrects the datatypes
    Checks for any missing values in the passenger count column and eliminates them
    Stores the clean data to a local path
    """
    df = pd.read_csv(local_path, low_memory=False)

    # Converting to correct datetime datatypes for the below columns
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # Store the cleaned df into the clean_data_local_path
    df.to_csv(clean_data_local_path, compression="infer")
    return df

def gcs_upload_to_bq(gs_path: str, bq_dataset: str, bq_table: str):
    pass


if __name__ == "__main__":
    main_flow()


