import datetime
import os

from pyiceberg.catalog import load_catalog, rest
import pyarrow as pa
import polars as pl

# Catalog defined in pyiceberg.yaml
# Must manually load env vars
# because "_" are replaced by "-" eg "AUTH__OAUTH2__CLIENT_SECRET" interpreted as "auth.oauth2.client-secret"
catalog = load_catalog()


def read_data(read_table: str) -> pl.DataFrame:
    table = catalog.load_table(read_table)
    return table.scan().to_polars()


def write_results(results: pl.DataFrame, write_table) -> None:
    # Add today as 'date'
    table = results.with_columns(date=pl.lit(datetime.date.today())).to_arrow()
    schema = table.schema

    # Primary keys must be "NOT NULL" whereas pyarrow fields are by default "optional"
    primary_keys = ["category", "date"]
    for pk in primary_keys:
        schema = schema.set(schema.get_field_index(pk), schema.field(pk).with_nullable(False))

    if catalog.table_exists(write_table):
        iceberg = catalog.load_table(write_table)

        iceberg.upsert(
            # apply schema on arrow table (.cast) with correct column ordering (.select)
            table.select(iceberg.schema().column_names).cast(
                pa.schema([schema.field(f) for f in iceberg.schema().column_names])
            ),
            join_cols=primary_keys,
        )

    else:
        iceberg = catalog.create_table(write_table, schema)

        with iceberg.update_schema() as update:
            update.set_identifier_fields(*primary_keys)

        iceberg.append(table.cast(pa.schema([schema.field(f) for f in iceberg.schema().column_names])))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--read", type=str, help="Read table, eg catalog.namespace.table")
    parser.add_argument("-w", "--write", type=str, help="Write table, eg catalog.namespace.table")
    args = parser.parse_args()

    df = read_data(args.read)

    counts = df.group_by("category").agg(pl.functions.len().cast(pl.Int64).alias("len"))
    print(counts.head())

    write_results(counts, args.write)
    print("data written")
