{{ config(materialized = 'sink') }}

CREATE SINK {{ this }} FROM {{ ref('mv_fraud_alerts_client') }} WITH (
    connector                    = 'kafka',
    properties.bootstrap.server  = '{{ env_var('KAFKA_BROKERS') }}',
    topic                        = 'avro-fraud-alerts-client',
    allow.auto.create.topics     = 'false'
)
FORMAT PLAIN ENCODE AVRO (
    force_append_only             = 'true',
    schema.registry               = '{{ env_var('SCHEMA_REGISTRY_URL') }}',
    schema.registry.name.strategy = 'topic_name_strategy'
)
