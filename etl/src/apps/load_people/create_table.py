import argparse
import random
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from apps.utils.spark_utils import get_spark_session


NAMES = [
    "Alice",
    "Bob",
    "Charly",
    "Dylan",
    "Elody",
    "Frank",
    "Gaby",
    "Hadrian",
    "Isa",
    "Juan",
    "Koala",
    "Lola",
    "Mona",
    "Noemy",
    "Ophely",
    "Patrick",
    "Quentin",
    "Rachel",
    "Stan",
    "Thibaut",
    "Ulrich",
    "Victoria",
    "Wan",
    "Xavier",
    "Zoro",
]

MIN_AGE = 1
MAX_AGE = 102



def generate_data(from_id: int, to_id: int) -> list:
    return [(idx, random.choice(NAMES), random.randint(MIN_AGE, MAX_AGE)) for idx in range(from_id, to_id)]


def add_fields(df: DataFrame) -> DataFrame:
    """Add some calculated fields"""
    return (
        df.withColumn(
            "category",
            f.when(f.col("age") < 15, f.lit("young"))
            .when(f.col("age") < 60, f.lit("adult"))
            .otherwise(f.lit("senior")),
        )
        .withColumn("birth", f.lit(2025) - f.col("age"))
        .withColumn(
            "created_at", f.current_timestamp()
        )  # if id already exists, it won't be updated, cf. `get_upsert_assignment`
        .withColumn("updated_at", f.col("created_at"))
    )


def get_upsert_assignment(columns: list[str]) -> dict:
    exclude_columns = ("id", "created_at")
    updated_columns = {column: f.col(f"source.{column}") for column in columns if column not in exclude_columns}
    print(
        f"Running upsert by updating following columns values (all except: {', '.join(exclude_columns)}): {list(updated_columns.keys())}"
    )
    return updated_columns


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", type=str, help="Complete table name, eg catalog.namespace.table")
    parser.add_argument("-fi", "--from_id", type=int, help="Generate data from this id, eg 0")
    parser.add_argument("-ti", "--to_id", type=int, help="Generate data until this id, eg 1000")

    args = parser.parse_args()
    table = args.table

    assert args.to_id > args.from_id, "to_id must be greater than from_id, eg --from_id 0 --to_id 1000"

    spark = get_spark_session()

    print(f"Generating {args.to_id - args.from_id} records, from {args.from_id} to {args.to_id}...")
    data = generate_data(args.from_id, args.to_id)

    sdf = spark.createDataFrame(data, "id: long, name: string, age: int")
    sdf = add_fields(sdf)
    sdf.printSchema()

    if not spark.catalog.tableExists(table):
        print("Table does not exist, creating from the data")
        sdf.writeTo(table).partitionedBy(f.col("category")).create()
    else:
        print("Table already exists, upsetting new data")
        (
            sdf.alias("source")
            .mergeInto(table=table, condition=f.col("source.id") == f.col(f"{table}.id"))
            .whenNotMatched()
            .insertAll()
            .whenMatched()
            .update(get_upsert_assignment(sdf.columns))
        ).merge()

    print("Data written, stopping")
    spark.stop()
