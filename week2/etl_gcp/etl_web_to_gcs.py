
# Creation Date: 8/2/2023 (D/M/YYYY)
# Topic: DE Zoomcamp 2.2.3 - ETL with GCP & Prefect
# Code Referenced from:
# video ~> https://www.youtube.com/watch?v=W-rMz_2GwqQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb
# repo ~> https://github.com/discdiver/prefect-zoomcamp/blob/main/flows/02_gcp/etl_web_to_gcs.py 


import pandas as pd

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Fix dtype issue (Data cleaning)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@task(log_prints=True, retries=3) # get logs
def clean(df = pd.DataFrame):
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2)) # print out top 2 rows
    print(f"columns: {df.types}") # print column data type
    print(f"rows: {len(df)}") # how many total rows we have
    return df

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Read taxi data from web into pandas dataframe

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@task(retries=3)
def fetch(dataset_url: str): 
    # if randint(0,1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Write dataframe out locally as parquet file

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Upload local parquet file to GCS

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@task()
def write_gcs(path: Path) -> None:
    gcs_block = GcsBucket.load("zoom-gcs") # insert own bucket name created in GCP
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path) 
    return

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Main ETL function

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@flow() # added @ flow and a new function 
def etl_web_to_gcs():
    color = "yellow"
    year = 2021
    month = 1

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()
