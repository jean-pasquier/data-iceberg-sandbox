# Infra

Docker compose

> Note: all services runs on default docker compose network for sake of simplicity.

```shell
# 1. Spin up persistent lakekeeper & minio
docker compose up -d

# 2. Spin up persistent Nimtable app
docker compose -f nimtable-docker-compose.yml up -d

# 3. Spin up an idle PySpark container with our ETL code mounted
docker compose -f pyspark-docker-compose.yml up -d
```

Visit UI provided:
* Lakekeeper: [http://localhost:8181](http://localhost:8181)
* Nimtable: [http://localhost:3000](http://localhost:3000)


## Spark ETL

Make sure to build the [ETL package](../etl/README.md)

```shell
# This generate data (with id's range as argument) to table: lakekeeper.poc_ns.people_partitioned
docker compose exec pyspark ./bin/spark-submit \
    --master 'local[2]' \
    --py-files /opt/spark/custom/dist/apps-0.1.0.tar.gz \
    /opt/spark/custom/src/apps/load_people/create_table.py \
    --from_id 1000000 --to_id 2000000
```

Run several times the spark job to create several iceberg snapshots (eg versions). Visit [Nimtable UI](http://localhost:3000) and check that table has been loaded (Data > Tables > `people_partitioned`) and number of records has increased (`Version Control` tab). Run the `Optimize table` to run compaction job (one time or scheduled) to solve data lake small files issue. The UI allows to preview data and running SQL queries.


Can run some queries using spark-sql console

```shell
docker compose exec pyspark ./bin/spark-sql \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.defaultCatalog=lakekeeper \
    --conf spark.sql.catalog.lakekeeper=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.lakekeeper.type=rest \
    --conf spark.sql.catalog.lakekeeper.uri=http://lakekeeper:8181/catalog \
    --conf spark.sql.catalog.lakekeeper.warehouse=test-iceberg \
    --conf spark.sql.catalog.lakekeeper.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.hadoop.hive.cli.print.header=true
spark-sql ()> SHOW CATALOGS;
spark-sql ()> SHOW TABLES in lakekeeper.poc_ns;
spark-sql ()> SELECT * FROM lakekeeper.poc_ns.people_partitioned LIMIT 10;
```


We can also query the data with Trino query engine.

```shell
# 1. Spin up Trino query engine
docker compose -f trino-docker-compose.yml up -d

# 2. Register the Iceberg catalog in Trino 
docker compose -f trino-docker-compose.yml exec trino trino -f /home/trino/init-catalog-trino.sql

# 3. Open a Trino SQL terminal to run some queries
docker compose -f trino-docker-compose.yml exec trino trino
trino> SHOW CATALOGS;
trino> SHOW SCHEMAS IN lakekeeper;  # schemas are namespaces, eg tables are identified as '<catalog>.<namespace>.<table>'
trino> SHOW CATALOGS;
trino> SELECT category, COUNT(*) FROM lakekeeper.poc_ns.people_partitioned GROUP BY 1;
```
