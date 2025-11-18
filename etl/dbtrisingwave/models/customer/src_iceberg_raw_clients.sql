{{ config(materialized='source') }}

CREATE SOURCE {{ this }}
WITH (
    connector = 'iceberg',
    database.name = 'customer',
    table.name = 'raw_clients',
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
    s3.region = 'local-01'
);
