import sys
import random
import time
import decimal
from datetime import datetime

import click
from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from apps.stream_transaction.utils import BROKERS, DEFAULT_TOPIC_CONFIG, schema_registry, read_file, delivery_report, _create_topic

TOPIC = "avro-transactions"
IDS = ("card_123", "card_234", "card_345", "card_456", "card_567", "card_678", "card_789")



@click.command()
@click.option("--every_sec", default=0.01, help="Time to wait before another loop.")
@click.option("--max_per_loop", default=3, help="Max number of records to send at each loop.")
@click.option("-min", "--min_amount", default=100, help="Minimum amount of transaction.")
@click.option("-max", "--max_amount", default=100, help="Maximum amount of transaction.")
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

    # Wait 200ms or 500 records to send micro batch
    config = {
        "bootstrap.servers": BROKERS,
        "linger.ms": "200",
        "batch.size": "500",
        "acks": "1",
        "request.timeout.ms": "60000",  # 1 min
        "delivery.timeout.ms": "300000",  # 5 min
        "queue.buffering.max.messages": "100000",
    }
    producer = Producer(config)

    click.echo(
        f"Starting producing randomly between 0 and {max_per_loop / every_sec:.2f} transactions/sec ({every_sec=}, {max_per_loop=})"
    )
    click.echo(f"Producer configuration: {config}")

    n_loops = 1

    while True:
        try:
            transactions = [
                {
                    "card_id": random.choice(IDS),
                    "amount": round(
                        decimal.Decimal(random.randrange(min_amount, max_amount) + random.random() * 100), 4
                    ),
                    "ts": datetime.now(),
                }
                for _ in range(random.randint(0, max_per_loop))
            ]

            if verbose:
                click.echo(f"Generated {len(transactions)}: {transactions}")

            for t in transactions:
                producer.produce(
                    topic=TOPIC,
                    key=key_str_serializer(t["card_id"]),
                    value=value_avro_serializer(t, SerializationContext(TOPIC, MessageField.VALUE)),
                    on_delivery=delivery_report,
                )
            producer.poll(every_sec / 2)  # use half-time to poll

            # Frequently flush producer buffer
            if n_loops % 1000 == 0:
                click.echo(f"Hitting {n_loops} loops, flushing producer buffer...")
                producer.flush()

            if n_loops >= sys.maxsize:
                click.echo(f"Hitting {n_loops} loops, resetting count to 0...")
                n_loops = 0

            time.sleep(every_sec / 2)  # use half-time to sleep
            n_loops += 1

        except KeyboardInterrupt:
            click.echo("Producer stopped: flushing")
            producer.flush()
            break


if __name__ == "__main__":
    _create_topic(TOPIC, DEFAULT_TOPIC_CONFIG)
    start_produce()
