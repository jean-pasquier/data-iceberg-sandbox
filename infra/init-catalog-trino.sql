CREATE CATALOG lakekeeper
  USING iceberg
  WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://lakekeeper:8181/catalog/',
    "iceberg.rest-catalog.vended-credentials-enabled" = 'false',
    "iceberg.rest-catalog.warehouse" = 'test-iceberg',
    "s3.endpoint" = 'http://minio:9000/',
    "s3.aws-access-key" = 'minio-root-user',
    "s3.aws-secret-key" = 'minio-root-pwd',
    "s3.region" = 'local-01',
    "s3.path-style-access" = 'true',
    "fs.native-s3.enabled" = 'true'
  );
