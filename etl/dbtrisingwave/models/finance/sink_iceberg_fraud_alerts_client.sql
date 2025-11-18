{{ config(materialized = 'sink') }}

CREATE SINK {{ this }} FROM {{ ref('mv_fraud_alerts_client') }} WITH (
    connector = 'iceberg',
    database.name = 'finance',
    table.name = 'fraud_alerts_client',
    type = 'append-only',
    force_append_only = 'true',
    create_table_if_not_exists = 'true',
    catalog.type = 'rest',
    catalog.uri = '{{ env_var('LAKEKEEPER_CATALOG_URI') }}',
    catalog.oauth2_server_uri = '{{ env_var('KEYCLOAK_TOKEN_ENDPOINT') }}',
    catalog.credential = 'risingwave:lzAW1xVUYiuxO4YpvnT6g4Df3wON88xf',
    catalog.scope = 'lakekeeper',
    warehouse.path = 'test-warehouse',
    s3.access.key = 'minio-root-user',
    s3.secret.key = 'minio-root-password',
    s3.path.style.access = 'true',
    s3.endpoint = 'http://minio:9000',
    s3.region = 'local-01',

    -- Iceberg table maintenance
    enable_compaction = true,
    compaction_interval_sec = 300,  -- 5 min
    enable_snapshot_expiration = true,
    snapshot_expiration_retain_last = 5
)
