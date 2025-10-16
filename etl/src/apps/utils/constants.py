import os

CATALOG_URL = os.getenv("CATALOG_URL", "http://localhost:8181/catalog")
WAREHOUSE = "test-iceberg"