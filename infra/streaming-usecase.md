# Complete data streaming use case

Test RisingWave features such as storing data, ingest/sink to Kafka & Iceberg and streaming processing.

Reference documentation: [https://docs.risingwave.com/sql/overview](https://docs.risingwave.com/sql/overview)

## 1 Quickstart

```sql
CREATE SCHEMA finance IF NOT EXISTS;

/* Raw input data: stored as risingwave default row-based table */
CREATE TABLE finance.credit_card_transactions (
    card_id VARCHAR,
    amount DECIMAL,
    ts TIMESTAMP
);

/* Insert few rows */

INSERT INTO finance.credit_card_transactions (card_id, amount, ts)
VALUES
    ('card_123', 1200.00, '2025-11-01 10:00:00'),
    ('card_123', 1800.00, '2025-11-01 10:00:20'),
    ('card_234', 1900.00, '2025-11-01 10:00:40'),
    ('card_345', 4000.00, '2025-11-01 10:01:00'),
    ('card_456', 950.00,  '2025-11-01 10:01:30')
;
```

Streaming transformation: identify frauds using window function 
<br/>See [risingwave doc on time windows](https://docs.risingwave.com/processing/sql/time-windows)

```sql
CREATE MATERIALIZED VIEW finance.fraud_alerts AS
SELECT
    card_id,
    window_start,
    window_end,
    SUM(amount) AS total_amount
FROM
    TUMBLE(
        finance.credit_card_transactions,   -- the source table
        ts,                                 -- the event timestamp column
        INTERVAL '1 minute'                 -- window size
    )
GROUP BY
    card_id, window_start, window_end
HAVING
    SUM(amount) > 5000  -- only alert if total spend exceeds $5000
;
```

Insert few more rows making sure to reach > 5000 per minute

```sql
INSERT INTO finance.credit_card_transactions (card_id, amount, ts)
VALUES
    ('card_123', 6000.00, '2025-11-01 10:00:50'),

    ('card_234', 2000.00, '2025-11-01 10:30:50'),
    ('card_234', 2000.00, '2025-11-01 10:30:51'),
    ('card_234', 2000.00, '2025-11-01 10:30:52'),

    ('card_456', 1000.00, '2025-11-01 09:02:00'),
    ('card_456', 1000.00, '2025-11-01 09:02:01'),
    ('card_456', 1000.00, '2025-11-01 09:02:02'),
    ('card_456', 1000.00, '2025-11-01 09:02:03'),
    ('card_456', 1000.00, '2025-11-01 09:02:04'),
    ('card_456', 1000.00, '2025-11-01 09:02:05'),
    ('card_456', 1000.00, '2025-11-01 09:02:06')
;

/* Should return few alerts */
SELECT * FROM finance.fraud_alerts LIMIT 100;
```

## 2 Sink the streaming MV to Kafka

First register a 'fraud_alerts_avro-value' subject in Kafka schema registry

```shell
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
http://localhost:18081/subjects/sink_frauds_alerts_avro-value/versions?normalize=true&format=resolved \
--data '{
  "schemaType": "AVRO",
  "schema": "{\"type\": \"record\", \"name\": \"FraudAlert\", \"namespace\": \"com.pasquier.jean\", \"fields\": [{\"name\": \"card_id\", \"type\": \"string\"}, {\"name\": \"window_start\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}}, {\"name\": \"window_end\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}}, {\"name\": \"total_amount\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 28}}]}"
}'
```

```sql
CREATE SINK finance.kafka_sink_fraud_alerts_avro
FROM finance.fraud_alerts
WITH (
   connector                    = 'kafka',
   properties.bootstrap.server  = 'kafka:9092',
   topic                        = 'fraud_alerts_avro',
   allow.auto.create.topics     = true  -- just for test, in prod create topic with tuned configuration
)
FORMAT PLAIN ENCODE AVRO (
    force_append_only             = 'true',
    schema.registry               = 'http://kafka:8081',
    schema.registry.name.strategy = 'topic_name_strategy'
);
```

Just to test kafka to risingwave -> read the kafka topic back

```sql
CREATE SOURCE finance.kafka_source_fraud_alerts_avro
WITH (
  connector                   = 'kafka',
  properties.bootstrap.server = 'kafka:9092',
  topic                       = 'fraud_alerts_avro',
)
FORMAT PLAIN ENCODE AVRO (
    schema.registry = 'http://kafka:8081'
);

-- Check if kafka_sink_fraud_alerts_avro -> kafka -> kafka_source_fraud_alerts_avro works well
SELECT * FROM finance.kafka_source_fraud_alerts_avro LIMIT 10;
```


## 3 Sink to Iceberg table


Either create a connection object of type iceberg (preferred way) or specify each parameter while creating iceberg-related risingwave objects (ie sink/source/managed tables)

```sql
CREATE CONNECTION public.lakekeeper_catalog_conn
WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog/',
    catalog.oauth2_server_uri = 'http://keycloak:8080/realms/iceberg/protocol/openid-connect/token',
    catalog.credential = 'risingwave:lzAW1xVUYiuxO4YpvnT6g4Df3wON88xf',
    catalog.scope = 'lakekeeper',
    warehouse.path = 'test-warehouse',
    s3.access.key = 'minio-root-user',
    s3.secret.key = 'minio-root-password',
    s3.path.style.access = 'true',
    s3.endpoint = 'http://minio:9000',
    s3.region = 'local-01'
);
```

Either create table in iceberg catalog first (eg in spark-sql console) or property: 'create_table_if_not_exists = 'true',

```sql
CREATE SINK finance.iceberg_sink_fraud_alerts
FROM finance.fraud_alerts
WITH (
    connector = 'iceberg',
    type = 'append-only',
    connection = public.lakekeeper_catalog_conn,
    database.name = 'finance',
    table.name = 'fraud_alerts',
    create_table_if_not_exists = 'true',
);
```


Check result in any iceberg query engine (Trino, spark sql, pyiceberg, etc.): `select * from lakekeeper.finance.fraud_alerts;`


## 4 Enrich streaming data with customer iceberg data using joins


Create a risingwave source on the spark-generated iceberg clients

```sql
CREATE SCHEMA IF NOT EXISTS customer;

CREATE SOURCE customer.raw_client
WITH (
  connector = 'iceberg',
  connection = public.lakekeeper_catalog_conn,
  database.name = 'customer',
  table.name = 'raw_client'
);
```

Create a relationship table between cards & clients. ENGINE = iceberg means it is stored as iceberg and thus can be queried from other query engines.

```sql
CREATE TABLE IF NOT EXISTS finance.credit_card_ownership (
     card_id VARCHAR,
     client_id BIGINT,
     PRIMARY KEY (card_id, client_id)
) ENGINE = iceberg;

INSERT INTO finance.credit_card_ownership
VALUES
    ('card_123', 123),  -- eg card_123 is owned by client 123
    ('card_234', 234),
    ('card_345', 345),
    ('card_456', 456),
    ('card_567', 567),
    ('card_678', 678),
    ('card_789', 789);
```

Now we can join frauds transactions with their corresponding client
<br/>See [risingwave doc on joins](https://docs.risingwave.com/processing/sql/joins)

```sql
CREATE MATERIALIZED VIEW finance.fraud_alerts_client
AS
SELECT c.id as client_id,
       c.name as client_name,
       c.category as client_category,
       f.card_id as card_id,
       f.window_start as window_start,
       f.window_end as window_end,
       f.total_amount as total_amount

FROM finance.fraud_alerts f

LEFT OUTER JOIN finance.credit_card_ownership co
ON f.card_id = co.card_id

LEFT OUTER JOIN customer.raw_client c
ON co.client_id = c.id
;
```

Inserting records manually into credit_card_transactions is quite boring, lets stream some records through kafka
<br/>Check `pykafka` that produces transactions to topic avro-transactions.

```sql
CREATE SOURCE finance.kafka_source_transactions

WITH (
    connector = 'kafka',
    topic = 'avro-transactions',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest',  -- start consuming from the oldest transactions
    properties.statistics.interval.ms = 30000, -- send stats every 30s
)
FORMAT PLAIN ENCODE AVRO (
   schema.registry = 'http://kafka:8081'
);
```

Insert kafka source into risingwave table

```sql
CREATE SINK finance.insert_kafka_into_transactions
INTO finance.credit_card_transactions
FROM finance.kafka_source_transactions;
```

Now the table `finance.credit_card_transactions` is populated continuously by transaction records.


We can query the fraud alerts enriched with clients data

```sql
SELECT * FROM finance.fraud_alerts_client LIMIT 10;
```

Note: if `pykafka` produces transaction with `card_id` that is not referenced in `finance.credit_card_ownership`, the left outer join will produce some NULL client information. To identify these, run

```sql
SELECT * 
FROM finance.fraud_alerts_client 
WHERE client_id IS NULL
LIMIT 10;
```

Insert "orphan" cards, eg

```sql
INSERT INTO finance.credit_card_ownership VALUES ('card_999', 999);
```

Risingwave automatically refresh the materialized view result using backfill mechanisms
<br/>See [internal risingwave doc on backfills](https://risingwavelabs.github.io/risingwave/design/backfill.html)

```sql
SELECT *
FROM finance.fraud_alerts_client
WHERE card_id = 'card_999'
LIMIT 10;
```

Should now return rows with client 999 information.
