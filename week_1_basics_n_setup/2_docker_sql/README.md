docker network create pg


docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --net=pg \
    --name pg-database \
    postgres:13


<!-- local port 8080 mapped to 80 port of pgadmin in the container -->
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --net=pg \
  --name pgadmin \
  dpage/pgadmin4



Target
To run the docker directly and pass parameters as:
- url to download csv file from
- postgres username
- postgres password
- postgres db name
- postgres tablename



URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
python ingest_data.py \
  --user root \
  --password root \
  --host localhost \
  --port 5432 \
  --dbname ny_taxi \
  --tablename yellow_taxi_data \
  --url ${URL}

docker build -t taxi_ingest:v001 .


URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz"
docker run -it \
  --net=pg \
  taxi_ingest:v001 \
    --user root \
    --password root \
    --host pg-database \
    --port 5432 \
    --dbname ny_taxi \
    --tablename yellow_taxi_data \
    --url ${URL}
  

GCP project ID: dtc-de-course-388001
export GOOGLE_APPLICATION_CREDENTIALS="~/.gcloud/dtc-de-course-388001-d14a628f6989.json"
gcloud auth application-default login


  