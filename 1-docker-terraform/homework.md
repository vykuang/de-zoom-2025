1. pip version of python:3.12.8: 24.3.1

```sh
docker pull python:3.12.8
docker run python:3.12.8 pip --version
```

2. db:5432 and postgres:5432; service name is defined as `db`, and container name as `postgres` for postgres, and so docker will resolve either to container's IP within the docker network

3. loading the data

```sh
docker buildx build -t ingest-nytaxi:green .
docker run \
    -v ./data:/data \
    -v ./1-docker-terraform/ingest_green.py:/app/ingest.py \
    --network 1-docker-terraform_nytaxi \
    ingest-nytaxi:green \
        --uri https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz --replace
```

```sql
select count(*)
from green
where trip_distance > 10
-- where trip_distance between 3.01 and 7
and lpep_pickup_datetime >= '2019-10-1'
and lpep_dropoff_datetime < '2019-11-1'
```

- using `iterator` and `chunk_size` lost 2 x `chunk_size` during ingestion for some reason
- <=1 104802
- 1-3 198924
- 3-7 109603
- 7-10 27,678
- >10  35189
- option b

4. longest trip for each day

```sql
select lpep_pickup_datetime
from green
where trip_distance = (
    select max(trip_distance) 
    from green)

-- 2019-10-31
```

5. largest pickup zones by `total_amount`

```sql
with t as (
	select sum(total_amount) as total, "PULocationID"
	from green 
	where date(lpep_pickup_datetime) = '2019-10-18'
	group by "PULocationID"
	having sum(total_amount)  > 13000
	)
select total, z."Zone" 
from t
left join zones z
on t."PULocationID" = z."LocationID"
order by total desc
```

Harlem North, East Harlem South, Morningside Heights

6. largest tip

```sql
select z."Zone"
from green g
left join zones z
on g."DOLocationID" = z."LocationID"
where g.tip_amount = (
	select max(tip_amount)
	from green g
	inner join zones z
	on g."PULocationID" = z."LocationID"
	where z."Zone" = 'East Harlem North'
)
-- JFK airport
```

7. tf workflow `terraform init, terraform apply -auto-approve, terraform destroy`