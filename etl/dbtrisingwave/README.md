# Risingwave with dbt

Add risingwave connection to `~/.dbt/profiles.yml` (current infra runs on docker with port exposed on localhost)

```yaml
risingwave:
  target: dev
  outputs:
    dev:
      type: risingwave
      name: prod
      host: localhost
      port: 4566
      dbname: dev
      schema: public
      user: root
      password: ''
      threads: 3
```

## Commands


### Prerequisites

Create the kafka source producer
```shell
cd ../../infra && docker compose -f kafka-docker-compose.yml -f pykafka-docker-compose.yml run --build pykafka -d && \
cd ../etl/dbtrisingwave
```


Create the kafka sink topic with custom config

```shell
TOPIC=avro-fraud-alerts-client
cd ../../infra && docker compose -f kafka-docker-compose.yml -f pykafka-docker-compose.yml run --build pykafka \
  /app/src/apps/stream_transaction/create_topic.py -n ${TOPIC} --n-parts 3 --conf cleanup.policy=compact --conf segment.ms=3600000 && \
  cd ../etl/dbtrisingwave

# Upload its schema on Kafka schema registry
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  http://localhost:18081/subjects/$TOPIC-value/versions?normalize=true&format=resolved \
  --data '{
    "schemaType": "AVRO",
    "schema": "{\"type\": \"record\",\"name\": \"FraudAlertClient\",\"namespace\": \"com.pasquier.jean\",\"fields\": [{\"name\": \"client_id\",\"type\": \"long\"},{\"name\": \"client_name\",\"type\": \"string\"},{\"name\": \"client_category\",\"type\": \"string\"},{\"name\": \"card_id\",\"type\": \"string\"},{\"name\": \"window_start\",\"type\": {\"type\": \"long\",\"logicalType\": \"timestamp-micros\"}},{\"name\": \"window_end\",\"type\": {\"type\": \"long\",\"logicalType\": \"timestamp-micros\"}},{\"name\": \"total_amount\",\"type\": {\"type\": \"bytes\",\"logicalType\": \"decimal\",\"precision\": 28}}]}"
  }';
```

Create the customer table with spark

```shell
cd ../../infra/ && docker compose -f docker-compose.yml -f pyspark-docker-compose.yml run --build \
  -e KEYCLOAK_TOKEN_ENDPOINT="http://keycloak:8080/realms/iceberg/protocol/openid-connect/token" \
  -e KEYCLOAK_CLIENT_ID=spark \
  -e KEYCLOAK_CLIENT_SECRET=2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52 \
  pyspark ./bin/spark-submit \
    --master 'local[*]' \
    --py-files /opt/spark/custom/dist/apps-0.1.0.tar.gz \
    /opt/spark/custom/src/apps/load_people/create_table.py \
    --table lakekeeper.customer.raw_clients \
    --from_id 0 --to_id 10000 && \
    cd ../etl/dbtrisingwave
```


dbt commands


```shell
# Make sure dbt & dbt-risingwave adapter are installed
cd ../ && uv sync --all-groups && source .venv/bin/activate && cd ./dbtrisingwave

# SQL commands run inside risingwave that is running in docker, so we can use docker compose hostnames
export LAKEKEEPER_CATALOG_URI="http://lakekeeper:8181/catalog"
export KEYCLOAK_TOKEN_ENDPOINT="http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
export KAFKA_BROKERS="kafka:9092"
export SCHEMA_REGISTRY_URL="http://kafka:8081"

# Compile SQL commands
dbt compile --log-level info

# Import static data (credit card ownership)
dbt seed --log-level info

# Execute SQL commands
dbt run --log-level info
```