"""Ingest exactly one raw ``odate`` into historical Bronze Delta tables."""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Serverless Python tasks execute workspace files through ``exec``. Add ``src``
# explicitly so the same code also works when invoked as a regular Python file.
SOURCE_ROOT = Path(sys.argv[0]).resolve().parents[1]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from common.batch import (  # noqa: E402
    TABLE_NAMES,
    log_status,
    parse_iso_date,
    replace_odate_partition,
    require_nonempty_partition,
)


ODATE_PATH_PATTERN = r"(?:^|/)odate=(\d{4}-\d{2}-\d{2})(?:/|$)"
RAW_SCHEMAS = {
    "patients": """
        patient_id STRING, full_name STRING, cpf STRING, email STRING,
        phone STRING, gender STRING, blood_type STRING, birth_date STRING,
        city STRING, state STRING, created_at STRING, _corrupt_record STRING
    """,
    "hospitals": """
        hospital_id STRING, hospital_name STRING, hospital_type STRING,
        state STRING, city STRING, capacity STRING, created_at STRING,
        _corrupt_record STRING
    """,
    "doctors": """
        doctor_id STRING, doctor_name STRING, crm STRING, specialty STRING,
        hospital_id STRING, created_at STRING, _corrupt_record STRING
    """,
    "diseases": """
        disease_id STRING, disease_name STRING, category STRING,
        severity_level STRING, created_at STRING, _corrupt_record STRING
    """,
    "attendance": """
        attendance_id STRING, patient_id STRING, doctor_id STRING,
        hospital_id STRING, disease_id STRING, attendance_date STRING,
        wait_time_minutes STRING, cost STRING, severity_score STRING,
        discharge_flag STRING, created_at STRING, _corrupt_record STRING
    """,
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--raw-root", required=True)
    parser.add_argument("--odate", required=True, type=parse_iso_date)
    return parser.parse_args()


def read_raw_csv(spark, raw_root: str, dataset_name: str, odate):
    """Read only the requested landing partition using a stable raw schema."""
    partition_path = (
        f"{raw_root.rstrip('/')}/{dataset_name}/odate={odate.isoformat()}"
    )
    return (
        spark.read.format("csv")
        .schema(RAW_SCHEMAS[dataset_name])
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("pathGlobFilter", "*.csv")
        .load(partition_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("odate", F.lit(odate).cast("date"))
    )


def run_ingestion(args, spark):
    for table_name in TABLE_NAMES:
        target_table = f"{args.catalog}.bronze.{table_name}"
        source_path = (
            f"{args.raw_root.rstrip('/')}/{table_name}/"
            f"odate={args.odate.isoformat()}"
        )
        log_status(
            "bronze",
            "table_started",
            table=target_table,
            source_path=source_path,
            odate=args.odate,
        )
        partition = require_nonempty_partition(
            read_raw_csv(spark, args.raw_root, table_name, args.odate),
            target_table,
            args.odate,
        )
        replace_odate_partition(spark, partition, target_table, args.odate)
        log_status(
            "bronze",
            "table_completed",
            table=target_table,
            odate=args.odate,
        )


def main():
    args = parse_args()
    log_status(
        "bronze",
        "task_started",
        catalog=args.catalog,
        odate=args.odate,
        table_count=len(TABLE_NAMES),
    )
    try:
        run_ingestion(args, SparkSession.builder.getOrCreate())
    except Exception as error:
        log_status(
            "bronze",
            "task_failed",
            catalog=args.catalog,
            odate=args.odate,
            error_type=type(error).__name__,
        )
        raise
    log_status(
        "bronze",
        "task_completed",
        catalog=args.catalog,
        odate=args.odate,
        processed_tables=len(TABLE_NAMES),
    )


if __name__ == "__main__":
    main()
