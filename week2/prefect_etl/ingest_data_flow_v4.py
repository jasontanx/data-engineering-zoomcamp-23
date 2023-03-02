
# Creation Date: 7/2/2023 (D/M/YYYY)
# Topic: DE Zoomcamp 2.2.2 - Introduction to Prefect Concepts
# Code Referenced from:
# Padilha -> https://github.com/padilha/de-zoomcamp/blob/master/week1/ingest_data.py
# https://www.youtube.com/watch?v=cdtN6dhp708&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=19
# Succesful run in local (mac)

import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time

from prefect import flow, task

@task(log_prints=True, retries=3) # able to receive metadata about upstearm dependencies
def ingest_data(user, password, host, port, db, table_name, url):

    if url.endswith('csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'
    
    os.system(f"wget {url} -O {csv_name}")
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    # change date columns from string to datetime format
    df.tpep_pickup_datetime = pd.to_datetime(df['tpep_pickup_datetime'])
    df.tpep_dropoff_datetime = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name='Ingest Flow') # added @ flow and a new function 
def main_flow():
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    ingest_data(user, password, host, port, db, table_name, csv_url)



if __name__ == '__main__':
    main_flow()

