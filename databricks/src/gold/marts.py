"""Analytics-ready HealthLake Gold dimensions, fact table, and daily KPI."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


CATALOG = spark.conf.get("healthlake.catalog")
SILVER = f"{CATALOG}.silver"


@dp.materialized_view(
    name="dim_patient",
    comment="PII-minimized patient dimension; direct identifiers are intentionally excluded.",
)
def dim_patient():
    return spark.read.table(f"{SILVER}.patients_current").select(
        "patient_id", "gender", "blood_type", "birth_date", "city", "state", "snapshot_date"
    )


@dp.materialized_view(name="dim_hospital", comment="Conformed hospital dimension.")
def dim_hospital():
    return spark.read.table(f"{SILVER}.hospitals_current").select(
        "hospital_id",
        "hospital_name",
        "hospital_type",
        "state",
        "city",
        "capacity",
        "snapshot_date",
    )


@dp.materialized_view(name="dim_doctor", comment="Conformed doctor dimension.")
def dim_doctor():
    return spark.read.table(f"{SILVER}.doctors_current").select(
        "doctor_id", "doctor_name", "specialty", "hospital_id", "snapshot_date"
    )


@dp.materialized_view(name="dim_disease", comment="Conformed disease dimension.")
def dim_disease():
    return spark.read.table(f"{SILVER}.diseases_current").select(
        "disease_id", "disease_name", "category", "severity_level", "snapshot_date"
    )


@dp.materialized_view(name="fact_attendance", comment="PII-minimized attendance fact table.")
@dp.expect_or_drop("attendance_date_present", "attendance_date IS NOT NULL")
def fact_attendance():
    return spark.read.table(f"{SILVER}.attendance_current").select(
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
        "snapshot_date",
    )


@dp.materialized_view(
    name="kpi_hospital_daily",
    comment="Daily volume, wait-time, cost, and discharge metrics by hospital.",
)
def kpi_hospital_daily():
    # These two names are datasets declared in this same pipeline. Resolving
    # them locally lets the pipeline build their lineage before the aggregate.
    attendance = spark.read.table("fact_attendance")
    hospitals = spark.read.table("dim_hospital")

    return (
        attendance.groupBy("hospital_id", "attendance_date")
        .agg(
            F.count("attendance_id").alias("attendance_count"),
            F.avg("wait_time_minutes").alias("avg_wait_time_minutes"),
            F.sum("cost").alias("total_cost"),
            F.avg(F.col("is_discharged").cast("double")).alias("discharge_rate"),
        )
        .join(
            hospitals.select("hospital_id", "hospital_name", "hospital_type", "state", "city"),
            on="hospital_id",
            how="left",
        )
        .select(
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
