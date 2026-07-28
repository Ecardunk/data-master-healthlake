"""Validated, typed current snapshots for the HealthLake Silver layer."""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


CATALOG = spark.conf.get("healthlake.catalog")


def mask_patient_pii():
    """Return irreversible presentation masks for direct patient identifiers."""
    cpf_digits = F.regexp_replace(F.col("cpf"), r"\\D", "")
    phone_digits = F.regexp_replace(F.col("phone"), r"\\D", "")

    return {
        "full_name": F.when(
            F.col("full_name").isNull(), F.lit(None).cast("string")
        ).otherwise(F.concat(F.substring(F.trim("full_name"), 1, 1), F.lit("***"))),
        "cpf": F.when(F.col("cpf").isNull(), F.lit(None).cast("string")).otherwise(
            F.concat(F.lit("***.***.***-"), F.substring(cpf_digits, -2, 2))
        ),
        "email": F.when(F.col("email").isNull(), F.lit(None).cast("string")).otherwise(
            F.concat(
                F.substring(F.lower(F.trim("email")), 1, 1),
                F.lit("***@"),
                F.regexp_extract(F.lower(F.trim("email")), r"@(.+)$", 1),
            )
        ),
        "phone": F.when(F.col("phone").isNull(), F.lit(None).cast("string")).otherwise(
            F.concat(F.lit("***-"), F.substring(phone_digits, -4, 4))
        ),
    }


def current_snapshot(table_name: str, key_column: str):
    """Return the deduplicated records from the most recent complete snapshot.

    The generator publishes full snapshots by ``odate``. Selecting the newest
    snapshot as a whole (rather than the newest record per key) lets churned
    source records disappear from Silver, matching the source-of-truth state.
    """
    source = spark.read.table(f"{CATALOG}.bronze.{table_name}")
    latest_snapshot_window = Window.partitionBy()
    latest_record_window = Window.partitionBy(key_column).orderBy(
        F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )

    return (
        source.withColumn("_latest_odate", F.max("odate").over(latest_snapshot_window))
        .where(F.col("odate") == F.col("_latest_odate"))
        .withColumn("_row_number", F.row_number().over(latest_record_window))
        .where(F.col("_row_number") == 1)
        .drop("_latest_odate", "_row_number", "_rescued_data")
    )


@dp.materialized_view(
    name="patients_current",
    comment="Latest validated patient snapshot with irreversible PII presentation masks.",
)
@dp.expect_or_drop("patient_id_present", "patient_id IS NOT NULL")
def patients_current():
    masks = mask_patient_pii()

    return current_snapshot("patients", "patient_id").select(
        F.col("patient_id").cast("bigint"),
        masks["full_name"].alias("full_name"),
        masks["cpf"].alias("cpf"),
        masks["email"].alias("email"),
        masks["phone"].alias("phone"),
        F.upper(F.trim("gender")).alias("gender"),
        F.upper(F.trim("blood_type")).alias("blood_type"),
        F.to_date("birth_date").alias("birth_date"),
        F.trim("city").alias("city"),
        F.upper(F.trim("state")).alias("state"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("odate").alias("snapshot_date"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


@dp.materialized_view(name="hospitals_current", comment="Latest validated hospital snapshot.")
@dp.expect_or_drop("hospital_id_present", "hospital_id IS NOT NULL")
def hospitals_current():
    return current_snapshot("hospitals", "hospital_id").select(
        F.col("hospital_id").cast("bigint"),
        F.trim("hospital_name").alias("hospital_name"),
        F.trim("hospital_type").alias("hospital_type"),
        F.upper(F.trim("state")).alias("state"),
        F.trim("city").alias("city"),
        F.col("capacity").cast("int").alias("capacity"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("odate").alias("snapshot_date"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


@dp.materialized_view(name="doctors_current", comment="Latest validated doctor snapshot.")
@dp.expect_or_drop("doctor_id_present", "doctor_id IS NOT NULL")
def doctors_current():
    return current_snapshot("doctors", "doctor_id").select(
        F.col("doctor_id").cast("bigint"),
        F.trim("doctor_name").alias("doctor_name"),
        F.col("crm").cast("bigint").alias("crm"),
        F.trim("specialty").alias("specialty"),
        F.col("hospital_id").cast("bigint").alias("hospital_id"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("odate").alias("snapshot_date"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


@dp.materialized_view(name="diseases_current", comment="Latest validated disease snapshot.")
@dp.expect_or_drop("disease_id_present", "disease_id IS NOT NULL")
def diseases_current():
    return current_snapshot("diseases", "disease_id").select(
        F.col("disease_id").cast("bigint"),
        F.trim("disease_name").alias("disease_name"),
        F.trim("category").alias("category"),
        F.col("severity_level").cast("int").alias("severity_level"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("odate").alias("snapshot_date"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


@dp.materialized_view(name="attendance_current", comment="Latest validated attendance snapshot.")
@dp.expect_or_drop("attendance_id_present", "attendance_id IS NOT NULL")
@dp.expect("attendance_date_present", "attendance_date IS NOT NULL")
def attendance_current():
    return current_snapshot("attendance", "attendance_id").select(
        F.col("attendance_id").cast("bigint"),
        F.col("patient_id").cast("bigint"),
        F.col("doctor_id").cast("bigint"),
        F.col("hospital_id").cast("bigint"),
        F.col("disease_id").cast("bigint"),
        F.to_timestamp("attendance_date").alias("attendance_timestamp"),
        F.to_date("attendance_date").alias("attendance_date"),
        F.col("wait_time_minutes").cast("int").alias("wait_time_minutes"),
        F.col("cost").cast("decimal(12,2)").alias("cost"),
        F.col("severity_score").cast("int").alias("severity_score"),
        (F.col("discharge_flag").cast("int") == 1).alias("is_discharged"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("odate").alias("snapshot_date"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )
