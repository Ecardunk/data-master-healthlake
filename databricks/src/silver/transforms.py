"""Promote one DQ-approved ``odate`` into historical Silver Delta tables."""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SOURCE_ROOT = Path(sys.argv[0]).resolve().parents[1]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from common.batch import (  # noqa: E402
    TABLE_NAMES,
    parse_iso_date,
    replace_odate_partition,
    require_gate_approval,
    require_nonempty_partition,
)
from silver.cleaning import clean_table, with_effective_odate  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--odate", required=True, type=parse_iso_date)
    return parser.parse_args()


def build_silver_partition(spark, catalog: str, table_name: str, odate):
    """Clean only one pushed-down Bronze partition."""
    source = with_effective_odate(
        spark.read.table(f"{catalog}.bronze.{table_name}")
    ).where(F.col("odate") == F.lit(odate))
    return clean_table(source, table_name)


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    require_gate_approval(
        spark, args.catalog, "bronze_to_silver", args.odate
    )

    for table_name in TABLE_NAMES:
        target_table = f"{args.catalog}.silver.{table_name}"
        partition = require_nonempty_partition(
            build_silver_partition(
                spark, args.catalog, table_name, args.odate
            ),
            target_table,
            args.odate,
        )
        replace_odate_partition(spark, partition, target_table, args.odate)


if __name__ == "__main__":
    main()
