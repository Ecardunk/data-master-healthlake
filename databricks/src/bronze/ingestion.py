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
    parse_iso_date,
    replace_odate_partition,
    require_nonempty_partition,
)


ODATE_PATH_PATTERN = r"(?:^|/)odate=(\d{4}-\d{2}-\d{2})(?:/|$)"
RAW_SCHEMAS = {
    "patients": """
        patient_id BIGINT, full_name STRING, cpf STRING, email STRING,
        phone STRING, gender STRING, blood_type STRING, birth_date STRING,
        city STRING, state STRING, created_at STRING, _corrupt_record STRING
    """,
    "hospitals": """
        hospital_id BIGINT, hospital_name STRING, hospital_type STRING,
        state STRING, city STRING, capacity DOUBLE, created_at STRING,
        _corrupt_record STRING
    """,
    "doctors": """
        doctor_id BIGINT, doctor_name STRING, crm DOUBLE, specialty STRING,
        hospital_id BIGINT, created_at STRING, _corrupt_record STRING
    """,
    "diseases": """
        disease_id BIGINT, disease_name STRING, category STRING,
        severity_level DOUBLE, created_at STRING, _corrupt_record STRING
    """,
    "attendance": """
        attendance_id BIGINT, patient_id BIGINT, doctor_id BIGINT,
        hospital_id BIGINT, disease_id BIGINT, attendance_date STRING,
        wait_time_minutes DOUBLE, cost DECIMAL(12,2), severity_score DOUBLE,
        discharge_flag DOUBLE, created_at STRING, _corrupt_record STRING
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
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("odate", F.lit(odate).cast("date"))
    )


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    for table_name in TABLE_NAMES:
        target_table = f"{args.catalog}.bronze.{table_name}"
        partition = require_nonempty_partition(
            read_raw_csv(spark, args.raw_root, table_name, args.odate),
            target_table,
            args.odate,
        )
        replace_odate_partition(spark, partition, target_table, args.odate)


if __name__ == "__main__":
    main()
