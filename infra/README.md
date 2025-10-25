# Infra

Docker compose

> Note: all services runs on default docker compose network for sake of simplicity.

```shell
# 1. Spin up datalake stack: Keycloak, MinIO, lakekeeper, etc. 
docker compose up -d

# 3. Spin up an idle PySpark container with our ETL code mounted
docker compose -f pyspark-docker-compose.yml up -d
```

Visit UI provided:
* Lakekeeper: [http://localhost:8181](http://localhost:8181)
* Nimtable: [http://localhost:3000](http://localhost:3000)





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



## User/machine auth management

Scenario implemented

1. Keycloak realm imported 

With following users:
* cdo (chief)
* Peter
* Anna

And following clients:
* Lakekeeper Catalog: standard flow (user auth on UI)
* lakekeeper-operator: client auth (machine auth)
* nimtable: client auth (machine auth)
* openfga: client auth (machine auth)
* Spark: client auth (machine auth)
* Starrocks: client auth (machine auth)
* Trino: standard flow (user auth on UI to run queries) + client auth (machine auth)

> Note: lakekeeper considers both users and clients as lakekeeper users: it has a field `user-type` = "application" | "human". user id is `oidc~<keycloak user id>` or `oidc~<keycloak client id>`. To be known by lakekeeper (ie user id stored in db):
> * the user must connect once with SSO (for keycloak users)
> * or hit POST REST API `{MANAGEMENT_URL}/v1/user` with a valid bearer token from Keycloak (for keycloak users and keycloak clients)


2. Lakekeeper bootstrapped (ie initialized) with following initial server roles:
* cdo is `admin`
* lakekeeper-operator is `operator`
* nimtable is `operator`
* cdo is `project_admin` of default project
* first warehouse `test-warehouse` is created
* a customer namespace is created with following permissions:
  * Peter has `ownership` of customer namespace
  * service-account-spark can `create` and `modify` sub objects



3. Create customer table and load some data into it with Spark ETL

Make sure to build the [ETL package](../etl/README.md)

```shell

# This generate data (with id's range as argument)
docker compose exec \
  -e KEYCLOAK_TOKEN_ENDPOINT="http://keycloak:8080/realms/iceberg/protocol/openid-connect/token" \
  -e KEYCLOAK_CLIENT_ID=spark \
  -e KEYCLOAK_CLIENT_SECRET=2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52 \
  pyspark ./bin/spark-submit \
    --master 'local[2]' \
    --py-files /opt/spark/custom/dist/apps-0.1.0.tar.gz \
    /opt/spark/custom/src/apps/load_people/create_table.py \
    --table lakekeeper.customer.raw_client \
    --from_id 0 --to_id 2000000
```

Run several times the spark job to create several iceberg snapshots (eg versions). Visit [Nimtable UI](http://localhost:3000) and check that table has been loaded (Data > Tables > `people_partitioned`) and number of records has increased (`Version Control` tab). Run the `Optimize table` to run compaction job (one time or scheduled) to solve data lake small files issue. The UI allows to preview data and running SQL queries.


Alternatively, can run some queries using spark-sql console

```shell
docker compose exec pyspark ./bin/spark-sql \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.defaultCatalog=lakekeeper \
    --conf spark.sql.catalog.lakekeeper=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.lakekeeper.type=rest \
    --conf spark.sql.catalog.lakekeeper.uri=http://lakekeeper:8181/catalog \
    --conf spark.sql.catalog.lakekeeper.warehouse=test-warehouse \
    --conf spark.sql.catalog.lakekeeper.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.lakekeeper.oauth2-server-uri=http://keycloak:8080/realms/iceberg/protocol/openid-connect/token \
    --conf spark.sql.catalog.lakekeeper.credential=spark:2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52 \
    --conf spark.sql.catalog.lakekeeper.scope=lakekeeper \
    --conf spark.hadoop.hive.cli.print.header=true
spark-sql ()> SHOW CATALOGS;
spark-sql ()> SHOW TABLES in lakekeeper.customer;
spark-sql ()> SELECT * FROM lakekeeper.customer.raw_client LIMIT 10;
```


4. Now let's allow Anna query the customer data with her favorite query engine: Trino

Access Jupyter notebooks on [localhost:8888](http://localhost:8888)

First cdo must setup trino catalog:
1. Assign `operator` permission to `service-account-trino` on project, eg using 01-Management notebook or using Lakekeeper UI (permission tab)
2. Run Trino-init notebook

Now Anna can perform Trino queries against `customer` namespace because she has `select` permission, examples:
```sql
SHOW CATALOGS;
SHOW TABLES IN customer;
SELECT * FROM customer.raw_client;
```

But she cannot create/modifying any table within `customer` namespace.


5. Let's create a `product` namespace for Anna


First cdo must create it -> eg using 01-Management notebook or using Lakekeeper UI (permission tab) and assign create/modify permissions to Anna

Now Anna can perform any Trino query against `product` namespace because she has `create`/`modify` permissions, examples:
```sql
CREATE TABLE IF NOT EXISTS product.raw_product (id INT, description VARCHAR, price DOUBLE);
INSERT INTO product.raw_product (id, description, price) VALUES (0, 'Product 1', 8.95), (1, 'Product 2', 17.95), (2, 'Product 3', 10);
SELECT * FROM product.raw_product;
```

6. We can assign `select` permission to Peter on table `product.raw_product`

> Note: Anna cannot grant permissions on tables she created because of a limitation -> service-account-trino has the ownership of tables created (user authentication was only to perform permission checks). 
> And Anna does not have the `manage_grants` privilege on the namespace (only create/modify). 

So cdo can assign permission to Peter on table `product.raw_product` using 01-Management notebook or using Lakekeeper UI (permission tab) and assign create/modify permissions to Anna

Open PyIceberg notebook and connect with Peter account (make sure to Logout from Lakekeeper).

Peter can only query `product.raw_product` with the python library of its choice: `pandas`, `pyarrow`, `polars`, `duckdb`, `ray`

And has no access to other tables in `product` namespace.

