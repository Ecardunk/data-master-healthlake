"""Runtime contract and idempotent Delta writes for one ``odate`` partition."""

import argparse
from datetime import date

from pyspark.sql import functions as F


TABLE_NAMES = ("patients", "hospitals", "doctors", "diseases", "attendance")


def parse_iso_date(value: str) -> date:
    """Parse the required business partition without falling back to the clock."""
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"Invalid odate {value!r}; expected YYYY-MM-DD"
        ) from error


def require_nonempty_partition(dataframe, table_name: str, odate: date):
    """Fail closed instead of publishing an empty layer partition."""
    if dataframe.limit(1).count() == 0:
        raise RuntimeError(
            f"{table_name} has no rows for odate={odate.isoformat()}"
        )
    return dataframe


def replace_odate_partition(spark, dataframe, table_name: str, odate: date):
    """Atomically replace exactly one Delta partition, making retries idempotent."""
    expected_odate = odate.isoformat()
    invalid_partition = dataframe.where(
        F.col("odate").isNull()
        | (F.col("odate") != F.lit(odate))
    )
    if invalid_partition.limit(1).count():
        raise RuntimeError(
            f"Refusing to write {table_name}: rows outside odate={expected_odate}"
        )

    if not spark.catalog.tableExists(table_name):
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .partitionBy("odate")
            .saveAsTable(table_name)
        )
        return

    partition_columns = spark.sql(
        f"DESCRIBE DETAIL {table_name}"
    ).select("partitionColumns").first()["partitionColumns"]
    if partition_columns != ["odate"]:
        raise RuntimeError(
            f"{table_name} must be PARTITIONED BY (odate); "
            f"found {partition_columns}"
        )

    existing_schema = spark.read.table(table_name).schema.simpleString()
    incoming_schema = dataframe.schema.simpleString()
    if existing_schema != incoming_schema:
        other_partition = (
            spark.read.table(table_name)
            .where(
                F.col("odate").isNull()
                | (F.col("odate") != F.lit(odate))
            )
            .limit(1)
            .count()
        )
        if other_partition:
            raise RuntimeError(
                f"Refusing schema replacement for {table_name}: "
                "the table contains historical odate partitions"
            )
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("odate")
            .saveAsTable(table_name)
        )
        return

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"odate = DATE '{expected_odate}'")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )


def require_gate_approval(spark, catalog: str, stage: str, odate: date):
    """Require the exact partition to have passed its preceding DQX gate."""
    control_table = f"{catalog}.observability.dq_promotion_control"
    if not spark.catalog.tableExists(control_table):
        raise RuntimeError(f"DQ promotion control table does not exist: {control_table}")

    approved = (
        spark.read.table(control_table)
        .where(
            (F.col("dq_stage") == F.lit(stage))
            & (F.col("odate") == F.lit(odate))
        )
        .limit(1)
        .count()
    )
    if not approved:
        raise RuntimeError(
            f"odate={odate.isoformat()} is not approved for stage {stage}"
        )
