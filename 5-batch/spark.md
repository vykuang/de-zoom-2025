# Spark and batch processing

- easy to manage
- retry
- scale at various points in the batch processing pipeline
- fits many applications (>80%); few actually need real-time data (<20%)
- cons:
    - delay
    - propagated by delay at each step

## intro to spark

- data processing engine
- distributed system
- based on java/scala
- pyspark
- lazy eval; only some methods trigger the pipeline to run

### example workflow

- raw data
- loaded as parquets in cloud object storage
- process with spark
- load into data warehouse
- or load into ML model

However if job can be expressed as SQL, that would be preferred. Spark is reserved for more complex expressions due to high overhead (development and maintenance)

## running spark

```py

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

this creates a server we can navigate to at localhost:4040 to view master UI

### spark cluster

- cluster has many workers, or *executors*, able to process many partitions in parallel
- files to be processed need to be *partitioned* to take advantage
    - `df.repartition(24)`

### spark dataframes


