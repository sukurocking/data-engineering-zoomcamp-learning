from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage
# import pandas as pd


@flow(log_prints=True)
def bigquery_load_cloud_storage_flow():
    projectid = "dtc-de-course-388001"
    # gcp_credentials = GcpCredentials(project=projectid)
    gcp_credentials = GcpCredentials.load("gcp-zoom-credentials")
    color = "yellow"
    gcs_bucket = "dtc-data-lake-dtc-de-course-388001"
    year = "2019"
    month = "02"
    
    result = bigquery_load_cloud_storage(
        dataset="trips_data_all",
        table="ny_trips",
        uri=f"gs://{gcs_bucket}/{color}/{year}-{month}.csv.gz",
        gcp_credentials=gcp_credentials,
        location="asia-south1"
    )
    return result

if __name__ == "__main__":
    bigquery_load_cloud_storage_flow()
