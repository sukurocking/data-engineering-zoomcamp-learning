FROM python:3.9

RUN pip install pandas, sqlalchemy, time, psycopg2, argparse, os

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py"]