"""Publish one DQ-approved ``odate`` into historical Gold Delta tables."""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_ROOT = Path(sys.argv[0]).resolve().parents[1]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

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


GOLD_COLUMNS = {
    "patients": (
        "patient_id",
        "gender",
        "blood_type",
        "birth_date",
        "city",
        "state",
        "odate",
    ),
    "hospitals": (
        "hospital_id",
        "hospital_name",
        "hospital_type",
        "state",
        "city",
        "capacity",
        "odate",
    ),
    "doctors": (
        "doctor_id",
        "doctor_name",
        "specialty",
        "hospital_id",
        "odate",
    ),
    "diseases": (
        "disease_id",
        "disease_name",
        "category",
        "severity_level",
        "odate",
    ),
    "attendance": (
        "attendance_id",
        "patient_id",
        "doctor_id",
        "hospital_id",
        "disease_id",
        "attendance_timestamp",
        "attendance_date",
        "wait_time_minutes",
        "cost",
        "severity_score",
        "is_discharged",
        "odate",
    ),
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--odate", required=True, type=parse_iso_date)
    return parser.parse_args()


def build_gold_partition(spark, catalog: str, table_name: str, odate):
    """Project the analytics contract from exactly one Silver partition."""
    return (
        spark.read.table(f"{catalog}.silver.{table_name}")
        .where(F.col("odate") == F.lit(odate))
        .select(*GOLD_COLUMNS[table_name])
    )


def build_daily_kpi(spark, catalog: str, odate):
    """Aggregate one partition and join the matching hospital snapshot."""
    attendance = (
        spark.read.table(f"{catalog}.gold.attendance")
        .where(F.col("odate") == F.lit(odate))
    )
    hospitals = (
        spark.read.table(f"{catalog}.gold.hospitals")
        .where(F.col("odate") == F.lit(odate))
    )

    return (
        attendance.groupBy("odate", "hospital_id", "attendance_date")
        .agg(
            F.count("attendance_id").alias("attendance_count"),
            F.avg("wait_time_minutes").alias("avg_wait_time_minutes"),
            F.sum("cost").alias("total_cost"),
            F.avg(F.col("is_discharged").cast("double")).alias("discharge_rate"),
        )
        .join(
            hospitals.select(
                "odate",
                "hospital_id",
                "hospital_name",
                "hospital_type",
                "state",
                "city",
            ),
            on=["odate", "hospital_id"],
            how="left",
        )
        .select(
            "odate",
            "attendance_date",
            "hospital_id",
            "hospital_name",
            "hospital_type",
            "state",
            "city",
            "attendance_count",
            F.round("avg_wait_time_minutes", 2).alias("avg_wait_time_minutes"),
            F.round("total_cost", 2).alias("total_cost"),
            F.round("discharge_rate", 4).alias("discharge_rate"),
        )
    )


def run_marts(args, spark):
    require_gate_approval(
        spark, args.catalog, "silver_to_gold", args.odate
    )

    for table_name in TABLE_NAMES:
        target_table = f"{args.catalog}.gold.{table_name}"
        log_status(
            "gold",
            "table_started",
            source_table=f"{args.catalog}.silver.{table_name}",
            table=target_table,
            odate=args.odate,
        )
        partition = require_nonempty_partition(
            build_gold_partition(
                spark, args.catalog, table_name, args.odate
            ),
            target_table,
            args.odate,
        )
        replace_odate_partition(spark, partition, target_table, args.odate)
        log_status(
            "gold",
            "table_completed",
            table=target_table,
            odate=args.odate,
        )

    kpi_table = f"{args.catalog}.gold.kpi_hospital_daily"
    log_status(
        "gold",
        "table_started",
        source_table=f"{args.catalog}.gold.attendance",
        table=kpi_table,
        odate=args.odate,
    )
    kpi_partition = require_nonempty_partition(
        build_daily_kpi(spark, args.catalog, args.odate),
        kpi_table,
        args.odate,
    )
    replace_odate_partition(
        spark, kpi_partition, kpi_table, args.odate
    )
    log_status(
        "gold",
        "table_completed",
        table=kpi_table,
        odate=args.odate,
    )


def main():
    args = parse_args()
    log_status(
        "gold",
        "task_started",
        catalog=args.catalog,
        odate=args.odate,
        table_count=len(TABLE_NAMES) + 1,
    )
    try:
        run_marts(args, SparkSession.builder.getOrCreate())
    except Exception as error:
        log_status(
            "gold",
            "task_failed",
            catalog=args.catalog,
            odate=args.odate,
            error_type=type(error).__name__,
        )
        raise
    log_status(
        "gold",
        "task_completed",
        catalog=args.catalog,
        odate=args.odate,
        processed_tables=len(TABLE_NAMES) + 1,
    )


if __name__ == "__main__":
    main()
