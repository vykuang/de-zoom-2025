import asyncio
import io
import os
import pandas as pd
from google.cloud import storage, bigquery
from google.api_core import exceptions
from dotenv import load_dotenv
from pathlib import Path
from urllib.request import urlretrieve
"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET

The line await loop.run_in_executor(None, urlretrieve, url, local_path) 
uses the event loop to run the urlretrieve function in an executor. 
The run_in_executor method allows blocking functions to be run in a 
separate thread or process, preventing them from blocking the main event loop. 
By passing None as the executor, the code uses the default thread pool executor
"""

load_dotenv("../.env")
# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")

async def create_external_table(project_id: str, dataset_id: str, table_id: str, gcs_uri: str) -> None:
    """
    Create an external table in BigQuery from GCS parquet files
    Args:
        project_id: GCP Project ID
        dataset_id: BigQuery dataset ID
        table_id: Name for the new external table
        gcs_uri: URI of GCS files (e.g. 'gs://bucket/path/to/*.parquet')
    """
    client = bigquery.Client(project=project_id)
    
    # Configure the external data source
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [gcs_uri]
    
    # Configure the table
    table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}")
    table.external_data_configuration = external_config
    
    # Create the external table
    table = client.create_table(table, exists_ok=True)
    print(
        f"Created external table {table.project}.{table.dataset_id}.{table.table_id}"
    )

async def blob_exists(bucket_name: str, blob_name: str) -> bool:
    """
    Check if a blob exists in the specified GCS bucket
    Args:
        bucket_name: Name of the GCS bucket
        blob_name: Name/path of the blob to check
    Returns:
        bool: True if blob exists, False otherwise
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    try:
        blob.reload()  # Attempts to fetch blob metadata
        return True
    except exceptions.NotFound:
        return False
    
async def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

async def download_file(service, year, month):
    """
    Download the NYC taxi data file.
    Args:
        service: Type of taxi service (e.g., 'fhv', 'green', 'yellow')
        year: Year of the data
        month: Month of the data
    Returns:
        Local path to the downloaded file
    """
    file_name = f"{service}_tripdata_{year}-{month:02d}.csv"
    url = f"{init_url}{service}/{file_name}"
    local_path = Path(f"data/{file_name}")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, urlretrieve, url, local_path)
    
    return str(local_path)

async def web_to_gcs(year, service):
    cache_dir = Path(f"../data/{service}")

    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = cache_dir / f"{service}_tripdata_{year}-{month}.csv.gz"
        if not file_name.exists():
            # download it using requests via a pandas df
            request_url = f"{init_url}{service}/{file_name.name}"
            print(f'retrieving from {request_url}')
            await urlretrieve(request_url, file_name)

            print(f"Local: {file_name}")
        else:
            print(f'{file_name} already exists')
        pq_name = cache_dir / file_name.name.replace('.csv.gz', '.parquet')
        if not pq_name.exists():
            # read it back into a parquet file
            df = pd.read_csv(file_name, compression='gzip')
            df = df.convert_dtypes() # locationIDs to ints
            df.to_parquet(pq_name, engine='pyarrow')
            print(f"Parquet: {pq_name}\n{df.dtypes}")
        else:
            print(f'{pq_name} already exists')
        # upload it to gcs 
        gcs_path = f"nytaxi/raw/{service}/{pq_name.name}"
        print(f"GCS: {gcs_path}")
        if not await blob_exists(BUCKET, gcs_path):
            await upload_to_gcs(BUCKET, gcs_path, pq_name)
        else:
            print(f"GCS: {gcs_path} already exists")

if __name__ == '__main__':
    await web_to_gcs('2019', 'fhv')
# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
