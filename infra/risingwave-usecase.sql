
--
-- #1 Quickstart
--

CREATE TABLE credit_card_transactions (
    card_id VARCHAR,
    amount DECIMAL,
    ts TIMESTAMP
);

INSERT INTO credit_card_transactions (card_id, amount, ts)
VALUES
    ('card_123', 1200.00, '2022-01-01 10:00:00'),
    ('card_123', 1800.00, '2022-01-01 10:00:20'),
    ('card_123', 1900.00, '2022-01-01 10:00:40'),
    ('card_456', 4000.00, '2022-01-01 10:01:00'),
    ('card_456', 950.00,  '2022-01-01 10:01:30')
;

CREATE MATERIALIZED VIEW fraud_alerts AS
SELECT
    card_id,
    window_start,
    window_end,
    SUM(amount) AS total_amount
FROM
    TUMBLE(
        credit_card_transactions,  -- the source table
        ts,                        -- the event timestamp column
        INTERVAL '1 minute'        -- window size
    )
GROUP BY
    card_id, window_start, window_end
HAVING
    SUM(amount) > 5000  -- only alert if total spend exceeds $5000
;


INSERT INTO credit_card_transactions (card_id, amount, ts)
VALUES ('card_123', 600.00, '2022-01-01 10:00:50');

INSERT INTO credit_card_transactions (card_id, amount, ts)
VALUES
    ('card_123', 2000.00, '2025-10-30 10:30:50'),
    ('card_123', 2000.00, '2025-10-30 10:30:51'),
    ('card_123', 2000.00, '2025-10-30 10:30:52'),
    ('card_456', 1000.00, '2025-10-30 09:02:00'),
    ('card_456', 1000.00, '2025-10-30 09:02:01'),
    ('card_456', 1000.00, '2025-10-30 09:02:02'),
    ('card_456', 1000.00, '2025-10-30 09:02:03'),
    ('card_456', 1000.00, '2025-10-30 09:02:04'),
    ('card_456', 1000.00, '2025-10-30 09:02:05'),
    ('card_456', 1000.00, '2025-10-30 09:02:06')
;


--
-- #2 Sink the streaming MV to Kafka topic
--

/**

First register a 'sink_frauds_alerts_avro-value' subject in Kafka schema registry

  curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  http://localhost:18081/subjects/sink_frauds_alerts_avro-value/versions?normalize=true&format=resolved
  --data '{
    "schemaType": "AVRO",
    "schema": "{\"type\": \"record\", \"name\": \"FraudAlert\", \"namespace\": \"com.pasquier.jean\", \"fields\": [{\"name\": \"card_id\", \"type\": \"string\"}, {\"name\": \"window_start\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}}, {\"name\": \"window_end\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}}, {\"name\": \"total_amount\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 28}}]}"
  }'

*/

CREATE SINK kafka_sink_fraud_alerts_avro
FROM fraud_alerts
WITH (
   connector                    = 'kafka',
   properties.bootstrap.server  = 'kafka:9092',
   topic                        = 'sink_frauds_alerts_avro',
   allow.auto.create.topics     = true
)
FORMAT PLAIN ENCODE AVRO (
    force_append_only             = 'true',
    schema.registry               = 'http://kafka:8081',
    schema.registry.name.strategy = 'topic_name_strategy'
);


-- #3 Source back to risingwave

CREATE SOURCE kafka_source_fraud_alerts_avro
WITH (
  connector                   = 'kafka',
  properties.bootstrap.server = 'kafka:9092',
  topic                       = 'sink_frauds_alerts_avro',
)
FORMAT PLAIN ENCODE AVRO (
    schema.registry = 'http://kafka:8081'
);


-- #4 Sink to Iceberg table
/*
       Either create a connection object of type iceberg (preferred way) or specify parameters while creating iceberg-related risingwave objects, ie sink/source/managed tables.
 */

CREATE CONNECTION lakekeeper_catalog_conn
WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog/',
    catalog.oauth2_server_uri = 'http://keycloak:8080/realms/iceberg/protocol/openid-connect/token',
    catalog.credential = 'risingwave:lzAW1xVUYiuxO4YpvnT6g4Df3wON88xf',
    catalog.scope = 'lakekeeper',
    warehouse.path = 'test-warehouse',
    s3.access.key = 'root-user',
    s3.secret.key = 'minio-root-password',
    s3.path.style.access = 'true',
    s3.endpoint = 'http://minio:9000',
    s3.region = 'local-01'
);




/*
    Either create table in iceberg catalog first (eg in spark-sql console) or property: 'create_table_if_not_exists = 'true',

    CREATE TABLE lakekeeper.finance.fraud_alerts (
        card_id VARCHAR(16),
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        total_amount DECIMAL
    );
 */

CREATE SINK iceberg_sink_fraud_alerts
FROM kafka_source_fraud_alerts_avro
WITH (
    connector = 'iceberg',
    type = 'append-only',
    connection = lakekeeper_catalog_conn,
    database.name = 'finance',
    table.name = 'fraud_alerts',
    create_table_if_not_exists = 'true',
);

----


/*
    Select data from iceberg, eg using spark-sql
 */

select * from lakekeeper.finance.fraud_alerts;