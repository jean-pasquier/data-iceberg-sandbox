import os
from pathlib import Path
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

import click
from confluent_kafka.schema_registry import SchemaRegistryClient


SCHEMA_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
DEFAULT_NUM_PARTITIONS = 6
DEFAULT_REPLICATION_FACTOR = 1
DEFAULT_TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "retention.ms": "86400000",  # 1 day
    "segment.ms": "86400000",  # 1 day
    "segment.bytes": "134217728",  # 128MiB
}


def _create_topic(topic_name: str, config: dict[str, str], n_parts: int = DEFAULT_NUM_PARTITIONS, repl_factor: int = DEFAULT_REPLICATION_FACTOR) -> None:
    admin = AdminClient({"bootstrap.servers": BROKERS})

    cluster_metadata = admin.list_topics()

    click.echo(f"Existing topics: {cluster_metadata.topics.keys()}")

    if topic_name not in cluster_metadata.topics:
        click.echo(f"Assuming topic {topic_name} does not exist")

        new_topic = NewTopic(topic_name, num_partitions=n_parts, replication_factor=repl_factor, config=config)
        click.echo(f"Topic creation spec: {new_topic}")

        futures = admin.create_topics(new_topics=[new_topic])  # dict[topic name, future]
        futures[topic_name].result()
    else:
        click.echo(f"Topic {topic_name} already exists")


def read_file(path: str) -> str:
    return (Path(__file__).parent / Path(path)).resolve().read_text()


def delivery_report(err, msg):
    if err is not None:
        click.echo(f"Delivery failed for record {msg.key()}: {err}")


schema_registry = SchemaRegistryClient({"url": SCHEMA_URL})
