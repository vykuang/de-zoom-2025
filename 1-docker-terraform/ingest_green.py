#! /usr/bin/env python
import argparse
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from time import time
from urllib.request import urlretrieve

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
    replace = conn_params.replace

    # look for existing
    if replace or not (Path('/data') / uri.name).exists():
        # os.system(f'wget --https-only {uri} -O /data/{uri.name}')
        path, headers = urlretrieve(conn_params.uri, f'/data/{uri.name}')
        print(f'file written to {path}')
        # response = requests.get(uri)
        # with open(f'/data/{uri.name}', 'w') as file:
        #     file.write(response.content)
    else:
        print(f'{uri.name} already exists')

    engine = create_engine(f'postgresql://{user}:{pw}@{host}:{port}/{db}')
    if 'lookup' in uri.name:
        df_zones = pd.read_csv(uri)
        df_zones.to_sql(name='zones', con=engine, if_exists='replace')
    else:
        df_iter = pd.read_csv(f'/data/{uri.name}', compression='gzip', iterator=True, chunksize=10000)
        # first chunk to create table
        df = next(df_iter)
        # df = pd.read_csv(f'/data/{uri.name}')
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        
        df.head(n=0).to_sql(name=tbl, con=engine, if_exists='replace')
        df.to_sql(name=tbl, con=engine, if_exists='append')
        nrows = len(df)
        print(f'inserted {len(df)} rows')

        # for df in df_iter:
        #     ta = time()
        #     df = next(df_iter)
        #     nrows += len(df)

        #     df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        #     df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        #     df.to_sql(name=tbl, con=engine, if_exists='append')
        #     tb = time()
        #     print(f'inserted {len(df)} rows in {tb-ta:.3f} sec')
    print(f'{nrows} rows ingested into postgres')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    opt = parser.add_argument
    opt('--user', default='postgres')
    opt('--pw', default='postgres')
    opt('--host', default='db')
    opt('--port', default=5432)
    opt('--db', default='ny_taxi')
    opt('--tbl', default='green')
    opt('--uri', type=str)
    opt('--replace', action='store_true')
    args = parser.parse_args()
    main(args)
