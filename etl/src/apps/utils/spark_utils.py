import os

import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from apps.utils.constants import CATALOG_URL, WAREHOUSE


SPARK_VERSION = pyspark.__version__
print(f"SPARK_VERSION: {SPARK_VERSION}")
SPARK_MINOR_VERSION = ".".join(SPARK_VERSION.split(".")[:2])
SPARK_APP_MASTER = os.getenv("SPARK_APP_MASTER", "local[2]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "iceberg-app")

ICEBERG_VERSION = "1.6.1"
ICEBERG_CATALOG = "lakekeeper"

KEYCLOAK_TOKEN_ENDPOINT = os.getenv("KEYCLOAK_TOKEN_ENDPOINT")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID")
KEYCLOAK_CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET")


def get_spark_session():
    config = {
        f"spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.lakekeeper.type": "rest",
        f"spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
        f"spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
        f"spark.sql.catalog.lakekeeper.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.lakekeeper.oauth2-server-uri": KEYCLOAK_TOKEN_ENDPOINT,
        "spark.sql.catalog.lakekeeper.credential": f"{KEYCLOAK_CLIENT_ID}:{KEYCLOAK_CLIENT_SECRET}",
        "spark.sql.catalog.lakekeeper.scope": ICEBERG_CATALOG,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": ICEBERG_CATALOG,
    }

    spark_config = SparkConf().setMaster(SPARK_APP_MASTER).setAppName(SPARK_APP_NAME)

    for k, v in config.items():
        spark_config = spark_config.set(k, v)

    return SparkSession.builder.config(conf=spark_config).getOrCreate()
