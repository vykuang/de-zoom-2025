#! /usr/bin/env python
import argparse
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from time import time

def main(conn_params):
    """
    ingest nytaxi csv.gz into postgres
    """
    user = conn_params.user
    pw = conn_params.pw
    host = conn_params.host
    port = conn_params.port
    db = conn_params.db
    tbl = conn_params.tbl
    uri = Path(conn_params.uri)

    # look for existing
    if not (Path('/data') / uri.name).exists():
        os.system(f'wget {uri} -O /data/{uri.name}')
    else:
        print(f'{uri.name} already exists')
    if (ext := '.'.join(uri.suffixes)) == '.csv.gz':
        cf = 'output.csv.gz'
    elif ext == '.csv':
        cf = 'output.csv'

    df_iter = pd.read_csv(f'/data/{uri.name}', iterator=True, chunksize=100000)
    # first chunk to create table
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    engine = create_engine(f'postgresql://{user}:{pw}@{host}:{port}/{db}')
    df.head(n=0).to_sql(name=tbl, con=engine, if_exists='replace')
    df.to_sql(name=tbl, con=engine, if_exists='append')

    for df in df_iter:
        ta = time()
        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=tbl, con=engine, if_exists='append')
        tb = time()
        print(f'insert another chunk in {tb-ta:.3f} sec')
    print('finish ingesting into postgres')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    opt = parser.add_argument
    opt('--user', default='postgres')
    opt('--pw', default='postgres')
    opt('--host', default='postgres')
    opt('--port', default=5432)
    opt('--db', default='ny_taxi')
    opt('--tbl', default='green')
    opt('--uri', type=str)
    args = parser.parse_args()
    main(args)
