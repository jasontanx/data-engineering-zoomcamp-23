#!/usr/bin/env python
# coding: utf-8

# --------------------------------------------------------------------#
# Created by: Jason
# Creation Date: 1/2/2023 (D/M/YYYY)
# Topic: DE Zoomcamp 1.2.2 - Ingesting NY Taxi Data to Postgres
# version 1 - coded on Mac (some code changes made)
# Code Referenced from:
# Padilha Notes & Repo-> https://github.com/padilha/de-zoomcamp/blob/master/week1/upload-data.ipynb 
# Data Engineering Zoomcamp -> https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup 
# --------------------------------------------------------------------#

# upload data into PostGres

# import necessary modules
import os
import pandas as pd
import argparse
from sqlalchemy import create_engine
import psycopg2
# python3 install psycopg2-binary
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    #url = params.url
    # path = params.path

    # donwload csv file
    # if url.endswith('.csv.gz'):
    #     csv_name = 'yellow_tripdata_2021-01.csv.gz'
    # else:
    #     csv_name = 'yellow_tripdata_2021-01.csv'

    # os.system(f"wget {url} -O {csv_name}")
    # csv_name = path  # the path of the file identified, create a var to store the csv

    parquet_file = '/Users/junshengtan/Desktop/Data_Engineering_Camp/Week1/Docker_Postgres/yellow_tripdata_2021-01.parquet'
    df = pd.read_parquet(parquet_file, engine = 'pyarrow')
    df.to_csv(parquet_file.replace('parquet', 'csv.gz'), index=False, compression='gzip')
    # csv_name= df.to_csv(parquet_file.replace('parquet', 'csv.gz'), index=False, compression='gzip')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    csv_name = '/Users/junshengtan/Desktop/Data_Engineering_Camp/Week1/Docker_Postgres/yellow_tripdata_2021-01.csv.gz'
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime  = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine,if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:

        try: 
            t_start = time()
            
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime  = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time()
            
            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Ingestion of CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    #parser.add_argument('--url', help='url of the csv file')
    #parser.add_argument('--path', help='path of the csv file')

    args = parser.parse_args()
    # print(args.accumulate(args.integers))
    main(args)

'''
python3 ingest_data.py \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--db=ny_taxi \
--table_name=yellow_taxi_trips 


docker build -t taxi_ingest:v001
'''
# after docker build
'''
docker run -it \
 --network=pg-network \
 taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips 

'''