python "02_Pyspark_sql.py" \
    --input_green "../../data_files/green/raw/*" \
    --input_yellow "../../data_files/yellow/raw/*" \
    --output "../../data_files/report/report_2019/"


# Parameters for usage in DataProc
--input_green=gs://dtc-data-lake-dtc-de-course-388001/green/*
--input_yellow=gs://dtc-data-lake-dtc-de-course-388001/yellow/*
--output=gs://dtc-data-lake-dtc-de-course-388001/report


gsutil cp 02_Pyspark_sql.py "gs://dtc-data-lake-dtc-de-course-388001/code"


# REST request for submitting a job
{
  "reference": {
    "jobId": "job-233db29b",
    "projectId": "dtc-de-course-388001"
  },
  "placement": {
    "clusterName": "dezoomcamp-f636"
  },
  "status": {
    "state": "RUNNING",
    "stateStartTime": "2023-07-22T11:52:15.853899Z"
  },
  "yarnApplications": [
    {
      "name": "testapp",
      "state": "RUNNING",
      "progress": 0.1,
      "trackingUrl": "http://dezoomcamp-f636-m.asia-south1-a.c.dtc-de-course-388001.internal.:8088/proxy/application_1690025996024_0001/"
    }
  ],
  "statusHistory": [
    {
      "state": "PENDING",
      "stateStartTime": "2023-07-22T11:52:15.482687Z"
    },
    {
      "state": "SETUP_DONE",
      "stateStartTime": "2023-07-22T11:52:15.516182Z"
    }
  ],
  "driverControlFilesUri": "gs://dataproc-staging-asia-south1-232165419990-j6do3mgk/google-cloud-dataproc-metainfo/33ec41ff-02f1-4658-bb80-72ab96f21629/jobs/job-233db29b/",
  "driverOutputResourceUri": "gs://dataproc-staging-asia-south1-232165419990-j6do3mgk/google-cloud-dataproc-metainfo/33ec41ff-02f1-4658-bb80-72ab96f21629/jobs/job-233db29b/driveroutput",
  "jobUuid": "a0978ef6-a459-4c8b-8e10-bd10ad2ceb6e",
  "pysparkJob": {
    "mainPythonFileUri": "gs://dtc-data-lake-dtc-de-course-388001/code/02_Pyspark_sql.py",
    "args": [
      "--input_green=gs://dtc-data-lake-dtc-de-course-388001/green/*",
      "--input_yellow=gs://dtc-data-lake-dtc-de-course-388001/yellow/*",
      "--output=gs://dtc-data-lake-dtc-de-course-388001/report"
    ]
  }
}


### Saving report to GCS
gcloud dataproc jobs submit pyspark \
    gs://dtc-data-lake-dtc-de-course-388001/code/02_Pyspark_sql.py \
    --cluster=dezoomcampcluster-787a \
    --region=asia-south1 \
    -- \
    --input_green=gs://dtc-data-lake-dtc-de-course-388001/green/\* \
    --input_yellow=gs://dtc-data-lake-dtc-de-course-388001/yellow/\* \
    --output=gs://dtc-data-lake-dtc-de-course-388001/report


### Saving to BigQuery
gcloud dataproc jobs submit pyspark \
    gs://dtc-data-lake-dtc-de-course-388001/code/03_Pyspark_sql_bigquery.py \
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.23.2.jar \
    --cluster=dezoomcampcluster-787a \
    --region=asia-south1 \
    -- \
    --input_green=gs://dtc-data-lake-dtc-de-course-388001/green/\* \
    --input_yellow=gs://dtc-data-lake-dtc-de-course-388001/yellow/\* \
    --output=trips_data_all.taxi_summary_report

