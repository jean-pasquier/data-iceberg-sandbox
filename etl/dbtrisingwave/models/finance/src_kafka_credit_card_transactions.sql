{{ config(materialized = 'table_with_connector') }}

CREATE TABLE {{ this }} WITH (
    connector = 'kafka',
    topic = 'avro-transactions',
    properties.bootstrap.server = '{{ env_var('KAFKA_BROKERS') }}',
    scan.startup.mode = 'earliest',                 -- start consuming from the oldest transactions
    properties.statistics.interval.ms = 30000,      -- send stats every 30s
)
FORMAT PLAIN ENCODE AVRO (
    schema.registry = '{{ env_var('SCHEMA_REGISTRY_URL') }}'
)
