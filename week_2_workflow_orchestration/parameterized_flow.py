import os
from datetime import timedelta
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_cloud_storage


@flow()
def main_flow():
    color = "yellow"
    year = "2020"
    month = "02"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"
    local_path = f"../data_files/{color}/{color}_tripdata_{year}-{month}.csv.gz"
    clean_data_local_path = f"../data_files/{color}/clean_{color}_tripdata_{year}-{month}.csv.gz"

    # Download from Web to Local path
    download_from_web_to_local(url, local_path)

    # Clean the dataframe
    clean_data(local_path, clean_data_local_path)

    # Upload from local path to GCS
    gs_file_name = f"{color}/clean_{color}_tripdata_{year}-{month}.csv.gz"
    upload_from_local_to_gcs(clean_data_local_path, gs_file_name)
    
    # Transfer data from GCS to BigQuery
    bq_dataset = "trips_data_all"
    bq_table = "ny_taxi"
    project_id = "dtc-de-course-388001"
    gs_data_lake = "dtc-data-lake-" + project_id
    gs_path_uri = f"gs://{gs_data_lake}/{color}/clean_{color}_tripdata_{year}-{month}.csv.gz"
    gcs_upload_to_bq(gs_path_uri, bq_dataset, bq_table)


# Download from Web to local
@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_from_web_to_local(url: str, local_path: str):
    # Downloads from a web url to local file
    os.system(f"wget -O {local_path} {url}")

# Clean the dataset
@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def clean_data(local_path: str, clean_data_local_path: str) -> None:
    """
    Corrects the datatypes
    Checks for any missing values in the passenger count column and eliminates them
    Stores the clean data to a local path
    """
    df = pd.read_csv(local_path, low_memory=False)

    # Converting to correct datetime datatypes for the below columns
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # Missing passenger_count records
    print(f'Pre: missing passenger count {df["passenger_count"].isna().sum()}')
    # Removing records with missing passenger_count
    df = df[df["passenger_count"].isna() == False]
    print(f'Post: missing passenger count {df["passenger_count"].isna().sum()}')
    
    # Store the cleaned df into the clean_data_local_path
    df.to_csv(clean_data_local_path, compression="infer")


# Upload from local to GCS
@task(retries=1, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def upload_from_local_to_gcs(df_local_path: str, gs_file_name:str, ):
    gcs_bucket = GcsBucket.load("gcs-bucket-zoom-new")
    gcs_bucket.upload_from_path(df_local_path, gs_file_name)


# Transfer data from GCS to BQ
@flow()
def gcs_upload_to_bq(gs_path_uri: str, bq_dataset: str, bq_table: str):
    # gcp_credentials = GcpCredentials(project="dtc-de-course-388001")
    gcp_credentials = GcpCredentials.load("gcp-zoom-credentials")
    result = bigquery_load_cloud_storage(
        dataset=bq_dataset,
        table=bq_table,
        uri=gs_path_uri,
        gcp_credentials=gcp_credentials,
        location="asia-south1"
    )
    return result


if __name__ == "__main__":
    main_flow()


