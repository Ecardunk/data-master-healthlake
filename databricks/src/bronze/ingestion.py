"""Incremental raw-file ingestion for the HealthLake Bronze layer."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


RAW_ROOT = spark.conf.get("healthlake.raw_root")


def read_raw_csv(dataset_name: str):
    """Read each ADF-delivered CSV only once and preserve its lineage."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .load(f"{RAW_ROOT}/{dataset_name}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "odate",
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"),
                    r"odate=(\\d{4}-\\d{2}-\\d{2})",
                    1,
                )
            ),
        )
    )


@dp.table(
    name="patients",
    comment="Source-aligned patient snapshots incrementally ingested from ADLS raw.",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_drop("patient_id_present", "patient_id IS NOT NULL")
def patients():
    return read_raw_csv("patients")


@dp.table(
    name="hospitals",
    comment="Source-aligned hospital snapshots incrementally ingested from ADLS raw.",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_drop("hospital_id_present", "hospital_id IS NOT NULL")
def hospitals():
    return read_raw_csv("hospitals")


@dp.table(
    name="doctors",
    comment="Source-aligned doctor snapshots incrementally ingested from ADLS raw.",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_drop("doctor_id_present", "doctor_id IS NOT NULL")
def doctors():
    return read_raw_csv("doctors")


@dp.table(
    name="diseases",
    comment="Source-aligned disease snapshots incrementally ingested from ADLS raw.",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_drop("disease_id_present", "disease_id IS NOT NULL")
def diseases():
    return read_raw_csv("diseases")


@dp.table(
    name="attendance",
    comment="Source-aligned attendance snapshots incrementally ingested from ADLS raw.",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_drop("attendance_id_present", "attendance_id IS NOT NULL")
def attendance():
    return read_raw_csv("attendance")
