from email.policy import default

import click

from apps.stream_transaction.utils import DEFAULT_TOPIC_CONFIG, _create_topic, DEFAULT_NUM_PARTITIONS, \
    DEFAULT_REPLICATION_FACTOR


@click.command()
@click.option("-n", "--name", help="Topic name")
@click.option("--n-parts", default=DEFAULT_NUM_PARTITIONS, help="Number of partitions")
@click.option("--repl-factor", default=DEFAULT_REPLICATION_FACTOR, help="Replication factor")
@click.option("--conf", multiple=True, help="Topic configuration e.g. --conf cleanup.policy=compact")
def create_topic(name: str, n_parts: int, repl_factor: int, conf: list[str]):
    parsed_conf = (c.split("=") for c in conf)
    config = DEFAULT_TOPIC_CONFIG | { c[0]: c[1] for c in parsed_conf }
    click.echo(f"Creating topic {name} with configuration {config}")

    _create_topic(topic_name=name, config=config, n_parts=n_parts, repl_factor=repl_factor)


if __name__ == "__main__":
    create_topic()