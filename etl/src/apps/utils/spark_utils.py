import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from apps.utils.constants import CATALOG_URL, WAREHOUSE


SPARK_CATALOG = "lakekeeper"

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = ".".join(SPARK_VERSION.split(".")[:2])
ICEBERG_VERSION = "1.6.1"


def get_spark_session():
    config = {
        f"spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.lakekeeper.type": "rest",
        f"spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
        f"spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
        f"spark.sql.catalog.lakekeeper.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": SPARK_CATALOG,
        "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    }

    spark_config = SparkConf().setMaster("local[2]").setAppName("app-iceberg")

    for k, v in config.items():
        spark_config = spark_config.set(k, v)

    return SparkSession.builder.config(conf=spark_config).getOrCreate()
