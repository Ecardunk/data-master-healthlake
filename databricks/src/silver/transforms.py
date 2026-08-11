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
    log_status,
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


def run_transforms(args, spark):
    require_gate_approval(
        spark, args.catalog, "bronze_to_silver", args.odate
    )

    for table_name in TABLE_NAMES:
        target_table = f"{args.catalog}.silver.{table_name}"
        log_status(
            "silver",
            "table_started",
            source_table=f"{args.catalog}.bronze.{table_name}",
            table=target_table,
            odate=args.odate,
        )
        partition = require_nonempty_partition(
            build_silver_partition(
                spark, args.catalog, table_name, args.odate
            ),
            target_table,
            args.odate,
        )
        replace_odate_partition(spark, partition, target_table, args.odate)
        log_status(
            "silver",
            "table_completed",
            table=target_table,
            odate=args.odate,
        )


def main():
    args = parse_args()
    log_status(
        "silver",
        "task_started",
        catalog=args.catalog,
        odate=args.odate,
        table_count=len(TABLE_NAMES),
    )
    try:
        run_transforms(args, SparkSession.builder.getOrCreate())
    except Exception as error:
        log_status(
            "silver",
            "task_failed",
            catalog=args.catalog,
            odate=args.odate,
            error_type=type(error).__name__,
            error=str(error),
        )
        raise
    log_status(
        "silver",
        "task_completed",
        catalog=args.catalog,
        odate=args.odate,
        processed_tables=len(TABLE_NAMES),
    )


if __name__ == "__main__":
    main()
