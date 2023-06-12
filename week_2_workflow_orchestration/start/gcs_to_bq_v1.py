from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage
# import pandas as pd


@flow
def example_bigquery_load_cloud_storage_flow():
    projectid = "dtc-de-course-388001"
    gcp_credentials = GcpCredentials(project=projectid)
    result = bigquery_load_cloud_storage(
        dataset="trips_data_all",
        table="ny_taxi",
        uri="gs://dtc-data-lake-dtc-de-course-388001/yellow/2019-02.csv.gz",
        gcp_credentials=gcp_credentials,
        location="asia-south1"
    )
    return result

example_bigquery_load_cloud_storage_flow()
