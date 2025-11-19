# Spark apps


## Init python env

```shell
brew install uv
uv sync --all-groups
```


## Write data with Spark

Spark session with Iceberg configuration: see `src/apps/utils/spark_utils.py`

Write into Iceberg table: see `src/apps/load_people/create_table.py` with method `pyspark.sql.DataFrame.writeTo`


## Analyze data with python pyiceberg & polars

Load data and play with any pyarrow compatible lib, here polars. See `src/apps/analytics/count_clients.py`


## Produce data to Kafka

Python kafka producer: see `src/apps/stream_transaction/produce_transactions.py`


## Stream data with Risingwave using dbt


Few tables and streaming materialized views are defined using dbt. See [dbt risingwave](./dbtrisingwave/README.md)
