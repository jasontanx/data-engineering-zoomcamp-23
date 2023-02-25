# Creation Date: 8/2/2023 (D/M/YYYY)
# Topic: DE Zoomcamp 2.2.4 - From Google Cloud Storage to Big Query
# Code Referenced from:
# video ~> https://www.youtube.com/watch?v=Cx5jt-V5sgE&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=21 
# repo ~> https://github.com/discdiver/prefect-zoomcamp/blob/main/flows/02_gcp/etl_gcs_to_bq.py 
# Succesful run in local (mac)


from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Download trip data from GCS

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@task() 
def extract_from_gcs(color: str, year: int, month: int) -> Path:

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Data cleaning example

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@task()
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Write DataFrame to BigQuery

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@task()
def write_bq(df: pd.DataFrame) -> None:

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds") # insert own bucket name created in GCP

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="prefect-sbx-community-eng", # insert GCP project ID
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Main ETL flow to load data into Big Query

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@flow() # added @ flow and a new function 
def etl_gcs_to_bq():
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
