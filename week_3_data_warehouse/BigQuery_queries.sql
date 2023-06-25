-- SELECT * FROM `dtc-de-course-388001.trips_data_all.ny_taxi` LIMIT 1000

# Record count from table
select count(1) from trips_data_all.ny_taxi;

# Creating an external table from a csv file loaded in GCS bucket
create or replace external table `trips_data_all.cleaned_ny_taxi_data`
OPTIONS (
  uris=['gs://dtc-data-lake-dtc-de-course-388001/yellow/clean_yellow_tripdata_2020-02.csv.gz'],
  format="CSV" 
);


-- Creating a non-partitioned table
CREATE TABLE `trips_data_all.ny_taxi_non_partitioned`
AS SELECT
    *
FROM `trips_data_all.cleaned_ny_taxi_data`;



-- Creating a parititioned table
CREATE TABLE `trips_data_all.ny_taxi_partitioned`
PARTITION BY DATE(tpep_pickup_datetime)
AS SELECT
    *
FROM `trips_data_all.cleaned_ny_taxi_data`;
