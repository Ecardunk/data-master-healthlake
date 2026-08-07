"""Validated current snapshots promoted only after the DQX gate succeeds."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from cleaning import clean_table, with_effective_odate


CATALOG = spark.conf.get("healthlake.catalog")
PROMOTION_CONTROL = f"{CATALOG}.observability.dq_promotion_control"


def approved_snapshot(table_name: str):
    """Return only the snapshot atomically approved for Bronze-to-Silver."""
    source = with_effective_odate(
        spark.read.table(f"{CATALOG}.bronze.{table_name}")
    ).alias("source")
    approved = (
        spark.read.table(PROMOTION_CONTROL)
        .where(F.col("dq_stage") == "bronze_to_silver")
        .select(F.col("odate").alias("_approved_odate"))
        .alias("approved")
    )
    return (
        source.join(
            F.broadcast(approved),
            F.col("source.odate") == F.col("approved._approved_odate"),
            "inner",
        )
        .drop("_approved_odate")
    )


@dp.materialized_view(
    name="patients_current",
    comment="Latest validated patient snapshot with irreversible PII presentation masks.",
)
@dp.expect_or_fail("patient_id_present", "patient_id IS NOT NULL")
def patients_current():
    return clean_table(approved_snapshot("patients"), "patients")


@dp.materialized_view(name="hospitals_current", comment="Latest validated hospital snapshot.")
@dp.expect_or_fail("hospital_id_present", "hospital_id IS NOT NULL")
def hospitals_current():
    return clean_table(approved_snapshot("hospitals"), "hospitals")


@dp.materialized_view(name="doctors_current", comment="Latest validated doctor snapshot.")
@dp.expect_or_fail("doctor_id_present", "doctor_id IS NOT NULL")
def doctors_current():
    return clean_table(approved_snapshot("doctors"), "doctors")


@dp.materialized_view(name="diseases_current", comment="Latest validated disease snapshot.")
@dp.expect_or_fail("disease_id_present", "disease_id IS NOT NULL")
def diseases_current():
    return clean_table(approved_snapshot("diseases"), "diseases")


@dp.materialized_view(name="attendance_current", comment="Latest validated attendance snapshot.")
@dp.expect_or_fail("attendance_id_present", "attendance_id IS NOT NULL")
@dp.expect_or_fail("attendance_date_present", "attendance_date IS NOT NULL")
def attendance_current():
    return clean_table(approved_snapshot("attendance"), "attendance")
