# Spark apps


## Init python env

```shell
uv sync --all-groups
uv build
```


## Write data with Spark

Spark session with Iceberg configuration: see `src/apps/utils/spark_utils.py`

Write into Iceberg table: see `src/apps/load_people/create_table.py` with method `pyspark.sql.DataFrame.writeTo`

