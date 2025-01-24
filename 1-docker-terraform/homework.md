1. pip version of python:3.12.8: 24.3.1

```sh
docker pull python:3.12.8
docker run python:3.12.8 pip --version
```

2. db:5432 and postgres:5432; service name is defined as `db`, and container name as `postgres` for postgres, and so docker will resolve either to container's IP within the docker network

3. loading the data

```sh
docker run \
    -v ./data:/data \
    -v ./1-docker-terraform/ingest_green.py:/app/ingest.py \
    --network 1-docker-terraform_nytaxi \
    ingest-nytaxi:green \
        --uri https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```
