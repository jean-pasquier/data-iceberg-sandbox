import os
from pathlib import Path

import click
from confluent_kafka.schema_registry import SchemaRegistryClient


SCHEMA_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")


def read_file(path: str) -> str:
    return (Path(__file__).parent / Path(path)).resolve().read_text()


def delivery_report(err, msg):
    if err is not None:
        click.echo(f"Delivery failed for record {msg.key()}: {err}")


schema_registry = SchemaRegistryClient({"url": SCHEMA_URL})
