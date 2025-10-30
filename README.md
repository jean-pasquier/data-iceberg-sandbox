# Iceberg sandbox

## Infra

### Overview

Rely on docker & docker compose

> Note: This commands creates a dozen of docker containers, make sure to have sufficient memory available for docker (6GB strict minimum, 16GB better)

```shell
docker compose -f observability-docker-compose.yml -f docker-compose.yml -f nimtable-docker-compose.yml -f trino-docker-compose.yml -f jupyter-docker-compose.yml -f kafka-docker-compose.yml -f risingwave-docker-compose.yml up --build -d
```

* Prometheus
* Grafana
* Postgres
* MinIO
* Openfga
* Keycloak
* Lakekeeper
* OPA
* Nimtable
* Trino
* Jupyter
* Kafka
* RisingWave

And some python or pyspark ETL with some code ready to run:

```shell
docker compose -f pyspark-docker-compose.yml run --build pyspark ./bin/spark-sql --help
docker compose -f pyiceberg-docker-compose.yml run --build pyiceberg
```

* PySpark
* PyIceberg

See [Playing with the data](#playing-with-the-data)


### Step-by-step guide

See complete scenario with user & data management on [infra](./infra/README.md)


## Playing with the data

Play with the data using PySpark or pyiceberg, see [etl](./etl/README.md)


## Next steps

* Create fine-grained service accounts for each data domain team (customer, product, finance, etc.) to implement the least privilege principle.
* Check [Production deployment Checklist](https://docs.lakekeeper.io/docs/nightly/production/)
* Run the [nimtable rust iceberg compaction service](https://github.com/nimtable/iceberg-compaction)
* Compare with competitors: [unitycatalog](https://github.com/unitycatalog/unitycatalog), [nessie](https://github.com/projectnessie/nessie), etc.
* Check streaming implementations: eg [fluss](https://github.com/apache/fluss)
