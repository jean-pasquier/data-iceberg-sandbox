import random
import time
import decimal
from datetime import datetime

import click
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from apps.stream_transaction.utils import BROKERS, schema_registry, read_file, delivery_report


TOPIC = "avro-transactions"
IDS = ("card_123", "card_234", "card_345", "card_456", "card_567", "card_678", "card_789")


def create_topic():
    admin = AdminClient({"bootstrap.servers": BROKERS})

    cluster_metadata = admin.list_topics()

    click.echo(f"Existing topics: {cluster_metadata.topics.keys()}")

    if TOPIC not in cluster_metadata.topics:
        click.echo(f"Assuming topic {TOPIC} does not exist")

        config = {
            "cleanup.policy": "delete",
            "retention.ms": "86400000",  # 1 day
            "segment.ms": "86400000",  # 1 day
            "segment.bytes": "134217728"  # 128MiB
        }

        new_topic = NewTopic(TOPIC, num_partitions=6, replication_factor=1, config=config)
        click.echo(f"Topic creation spec: {new_topic}")

        futures = admin.create_topics(new_topics=[new_topic])  # dict[topic name, future]
        click.echo(f"Create topic request result: {futures[TOPIC].result()}")
    else:
        click.echo(f"Topic {TOPIC} already exists")
        # new_partitions = NewPartitions(topic=TOPIC, new_total_count=6)
        # futures = admin.create_partitions(new_partitions=[new_partitions])
        # click.echo(f"Create partitions request result: {futures[TOPIC].result()}")


@click.command()
@click.option('--every_sec', default=0.01, help='Time to wait before another loop.')
@click.option('--max_per_loop', default=3, help='Max number of records to send at each loop.')
@click.option('-min', '--min_amount', default=100, help='Minimum amount of transaction.')
@click.option('-max', '--max_amount', default=100, help='Maximum amount of transaction.')
@click.option("-v", "--verbose", is_flag=True, default=False, help="Verbose mode.")
def start_produce(every_sec: float, max_per_loop, min_amount: int, max_amount: int, verbose: bool = False):
    transaction_schema = read_file("transaction.avsc")

    if verbose:
        click.echo("Transaction record schema:")
        click.echo(transaction_schema)

    value_avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry, schema_str=transaction_schema, conf={"normalize.schemas": True}
    )
    key_str_serializer = StringSerializer("utf_8")

    # Wait 200ms or 100 records to send micro batch
    config = {"bootstrap.servers": BROKERS, "linger.ms": "200", "batch.size": "100", "acks": "1"}
    producer = Producer(config)

    click.echo(f"Starting producing 0 to {max_per_loop/every_sec:.2f} transactions/sec ({every_sec=} {max_per_loop=})")
    click.echo(f"Configuration: {config}")

    while True:
        try:
            producer.poll(0.0)

            transactions = [
                {
                    "card_id": random.choice(IDS),
                    "amount": round(decimal.Decimal(random.randrange(min_amount, max_amount) + random.random() * 100), 4),
                    "ts": datetime.now(),
                }
                for _ in range(random.randint(0, max_per_loop))
            ]

            if verbose:
                click.echo(f"Generated {len(transactions)}:", transactions)

            for t in transactions:
                producer.produce(
                    topic=TOPIC,
                    key=key_str_serializer(t["card_id"]),
                    value=value_avro_serializer(t, SerializationContext(TOPIC, MessageField.VALUE)),
                    on_delivery=delivery_report,
                )

            time.sleep(every_sec)

        except KeyboardInterrupt:
            click.echo("Producer stopped: flushing")
            producer.flush()
            break


if __name__ == "__main__":

    create_topic()
    start_produce()
