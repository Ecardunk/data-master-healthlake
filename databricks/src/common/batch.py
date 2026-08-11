"""Runtime contract and idempotent Delta writes for one ``odate`` partition."""

import argparse
import json
from datetime import date, datetime, timezone

from pyspark.sql import functions as F


TABLE_NAMES = ("patients", "hospitals", "doctors", "diseases", "attendance")


def log_status(component: str, event: str, **fields):
    """Emit one immediately visible, PII-free event to the Databricks task output."""
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "component": component,
        "event": event,
        **fields,
    }
    print(
        f"[healthlake] {json.dumps(payload, default=str, sort_keys=True)}",
        flush=True,
    )


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
    log_status(
        "batch_validation",
        "nonempty_check_started",
        table=table_name,
        odate=odate,
    )
    if dataframe.limit(1).count() == 0:
        log_status(
            "batch_validation",
            "nonempty_check_failed",
            table=table_name,
            odate=odate,
        )
        raise RuntimeError(
            f"{table_name} has no rows for odate={odate.isoformat()}"
        )
    log_status(
        "batch_validation",
        "nonempty_check_passed",
        table=table_name,
        odate=odate,
    )
    return dataframe


def replace_odate_partition(spark, dataframe, table_name: str, odate: date):
    """Atomically replace exactly one Delta partition, making retries idempotent."""
    expected_odate = odate.isoformat()
    log_status(
        "delta_writer",
        "partition_validation_started",
        table=table_name,
        odate=expected_odate,
    )
    invalid_partition = dataframe.where(
        F.col("odate").isNull()
        | (F.col("odate") != F.lit(odate))
    )
    if invalid_partition.limit(1).count():
        log_status(
            "delta_writer",
            "partition_validation_failed",
            table=table_name,
            odate=expected_odate,
        )
        raise RuntimeError(
            f"Refusing to write {table_name}: rows outside odate={expected_odate}"
        )
    log_status(
        "delta_writer",
        "partition_validation_passed",
        table=table_name,
        odate=expected_odate,
    )

    if not spark.catalog.tableExists(table_name):
        log_status(
            "delta_writer",
            "table_create_started",
            table=table_name,
            odate=expected_odate,
        )
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .partitionBy("odate")
            .saveAsTable(table_name)
        )
        log_status(
            "delta_writer",
            "table_create_completed",
            table=table_name,
            odate=expected_odate,
        )
        return

    partition_columns = spark.sql(
        f"DESCRIBE DETAIL {table_name}"
    ).select("partitionColumns").first()["partitionColumns"]
    if partition_columns != ["odate"]:
        log_status(
            "delta_writer",
            "partition_contract_failed",
            table=table_name,
            expected_partition_columns=["odate"],
            actual_partition_columns=partition_columns,
        )
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
            log_status(
                "delta_writer",
                "schema_recovery_blocked",
                table=table_name,
                odate=expected_odate,
                reason="historical_partitions_present",
            )
            raise RuntimeError(
                f"Refusing schema replacement for {table_name}: "
                "the table contains historical odate partitions"
            )
        log_status(
            "delta_writer",
            "schema_recovery_started",
            table=table_name,
            odate=expected_odate,
        )
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("odate")
            .saveAsTable(table_name)
        )
        log_status(
            "delta_writer",
            "schema_recovery_completed",
            table=table_name,
            odate=expected_odate,
        )
        return

    log_status(
        "delta_writer",
        "partition_replace_started",
        table=table_name,
        odate=expected_odate,
    )
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"odate = DATE '{expected_odate}'")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
    log_status(
        "delta_writer",
        "partition_replace_completed",
        table=table_name,
        odate=expected_odate,
    )


def require_gate_approval(spark, catalog: str, stage: str, odate: date):
    """Require the exact partition to have passed its preceding DQX gate."""
    control_table = f"{catalog}.observability.dq_promotion_control"
    log_status(
        "promotion_gate",
        "approval_check_started",
        stage=stage,
        odate=odate,
        control_table=control_table,
    )
    if not spark.catalog.tableExists(control_table):
        log_status(
            "promotion_gate",
            "approval_check_failed",
            stage=stage,
            odate=odate,
            reason="control_table_missing",
        )
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
        log_status(
            "promotion_gate",
            "approval_check_failed",
            stage=stage,
            odate=odate,
            reason="partition_not_approved",
        )
        raise RuntimeError(
            f"odate={odate.isoformat()} is not approved for stage {stage}"
        )
    log_status(
        "promotion_gate",
        "approval_check_passed",
        stage=stage,
        odate=odate,
    )
