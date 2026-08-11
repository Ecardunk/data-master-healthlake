"""Fail-closed DQX gates for one cleaned HealthLake snapshot at a time."""

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Serverless Spark Python tasks execute workspace files through ``exec`` and do
# not define ``__file__``. The launcher does preserve the script path in argv.
SOURCE_ROOT = Path(sys.argv[0]).resolve().parents[1]
SILVER_SOURCE = SOURCE_ROOT / "silver"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))
if str(SILVER_SOURCE) not in sys.path:
    sys.path.insert(0, str(SILVER_SOURCE))

from common.batch import log_status  # noqa: E402
from cleaning import clean_table, with_effective_odate  # noqa: E402


spark = SparkSession.builder.getOrCreate()


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"Invalid odate {value!r}; expected YYYY-MM-DD"
        ) from error


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage", choices=["bronze_to_silver", "silver_to_gold"], required=True
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--odate", required=True, type=parse_iso_date)
    return parser.parse_args()


def not_null(name: str, column: str) -> DQRowRule:
    return DQRowRule(
        name=name,
        criticality="error",
        check_func=check_funcs.is_not_null,
        column=column,
    )


def sql_rule(name: str, expression: str, message: str) -> DQRowRule:
    return DQRowRule(
        name=name,
        criticality="error",
        check_func=check_funcs.sql_expression,
        check_func_kwargs={"expression": expression, "msg": message},
    )


def unique_key(name: str, key: str) -> DQRowRule:
    return sql_rule(
        f"{name}_unique",
        f"COUNT(*) OVER (PARTITION BY {key}) = 1",
        f"{key} must be unique within the cleaned source snapshot",
    )


def rules_for(stage: str, table_name: str):
    if stage == "bronze_to_silver":
        rules = {
            "patients": [
                not_null("patient_id_present", "patient_id"),
                unique_key("patient_id", "patient_id"),
                sql_rule(
                    "birth_date_not_future",
                    "birth_date IS NOT NULL AND birth_date <= current_date()",
                    "birth_date must be present and not be in the future",
                ),
                sql_rule(
                    "gender_allowed",
                    "gender IS NOT NULL AND gender IN ('M', 'F')",
                    "gender must be M or F",
                ),
                sql_rule(
                    "state_is_uf",
                    "state IS NOT NULL AND state RLIKE '^[A-Z]{2}$'",
                    "state must contain a two-letter Brazilian UF",
                ),
            ],
            "hospitals": [
                not_null("hospital_id_present", "hospital_id"),
                unique_key("hospital_id", "hospital_id"),
                sql_rule(
                    "capacity_in_range",
                    "capacity IS NOT NULL AND capacity BETWEEN 1 AND 2000",
                    "capacity must be between 1 and 2000",
                ),
                sql_rule(
                    "state_is_uf",
                    "state IS NOT NULL AND state RLIKE '^[A-Z]{2}$'",
                    "state must contain a two-letter Brazilian UF",
                ),
            ],
            "doctors": [
                not_null("doctor_id_present", "doctor_id"),
                unique_key("doctor_id", "doctor_id"),
                sql_rule(
                    "crm_positive",
                    "crm IS NOT NULL AND crm > 0",
                    "crm must be a positive value",
                ),
                not_null("doctor_hospital_present", "hospital_id"),
            ],
            "diseases": [
                not_null("disease_id_present", "disease_id"),
                unique_key("disease_id", "disease_id"),
                sql_rule(
                    "severity_in_range",
                    "severity_level IS NOT NULL AND severity_level BETWEEN 1 AND 5",
                    "severity_level must be between 1 and 5",
                ),
            ],
            "attendance": [
                not_null("attendance_id_present", "attendance_id"),
                unique_key("attendance_id", "attendance_id"),
                not_null("attendance_patient_present", "patient_id"),
                not_null("attendance_doctor_present", "doctor_id"),
                not_null("attendance_hospital_present", "hospital_id"),
                not_null("attendance_disease_present", "disease_id"),
                sql_rule(
                    "attendance_date_valid",
                    "attendance_timestamp IS NOT NULL AND attendance_timestamp <= current_timestamp()",
                    "attendance_date must be present and not be in the future",
                ),
                sql_rule(
                    "wait_time_in_range",
                    "wait_time_minutes IS NULL OR wait_time_minutes BETWEEN 0 AND 300",
                    "wait_time_minutes must be between 0 and 300 when supplied",
                ),
                sql_rule(
                    "cost_non_negative",
                    "cost IS NULL OR cost >= 0",
                    "cost cannot be negative",
                ),
                sql_rule(
                    "severity_in_range",
                    "severity_score IS NOT NULL AND severity_score BETWEEN 1 AND 5",
                    "severity_score must be between 1 and 5",
                ),
                sql_rule(
                    "discharge_flag_allowed",
                    "discharge_flag IS NULL OR discharge_flag IN (0, 1)",
                    "discharge_flag must be 0 or 1 when supplied",
                ),
            ],
        }
    else:
        rules = {
            "patients": [
                not_null("patient_id_present", "patient_id"),
                unique_key("patient_id", "patient_id"),
                not_null("odate_present", "odate"),
            ],
            "hospitals": [
                not_null("hospital_id_present", "hospital_id"),
                unique_key("hospital_id", "hospital_id"),
                sql_rule(
                    "capacity_in_range",
                    "capacity IS NOT NULL AND capacity BETWEEN 1 AND 2000",
                    "capacity must be between 1 and 2000",
                ),
            ],
            "doctors": [
                not_null("doctor_id_present", "doctor_id"),
                unique_key("doctor_id", "doctor_id"),
                not_null("hospital_id_present", "hospital_id"),
            ],
            "diseases": [
                not_null("disease_id_present", "disease_id"),
                unique_key("disease_id", "disease_id"),
                sql_rule(
                    "severity_in_range",
                    "severity_level IS NOT NULL AND severity_level BETWEEN 1 AND 5",
                    "severity_level must be between 1 and 5",
                ),
            ],
            "attendance": [
                not_null("attendance_id_present", "attendance_id"),
                unique_key("attendance_id", "attendance_id"),
                not_null("attendance_date_present", "attendance_date"),
                not_null("patient_id_present", "patient_id"),
                not_null("doctor_id_present", "doctor_id"),
                not_null("hospital_id_present", "hospital_id"),
                not_null("disease_id_present", "disease_id"),
                sql_rule(
                    "cost_non_negative",
                    "cost IS NULL OR cost >= 0",
                    "cost cannot be negative",
                ),
                sql_rule(
                    "severity_in_range",
                    "severity_score IS NOT NULL AND severity_score BETWEEN 1 AND 5",
                    "severity_score must be between 1 and 5",
                ),
            ],
        }

    return rules[table_name]


def source_schema(stage: str) -> str:
    return "bronze" if stage == "bronze_to_silver" else "silver"


def snapshot_column(stage: str) -> str:
    return "odate"


def tables_for(stage: str):
    return ["patients", "hospitals", "doctors", "diseases", "attendance"]


def prepare_for_checks(dataframe, stage: str, table_name: str):
    """Run every deterministic cleaning operation before evaluating DQ rules."""
    if stage == "bronze_to_silver":
        return clean_table(dataframe, table_name, include_quality_columns=True)
    return dataframe


def mask_sensitive_columns(dataframe, table_name: str):
    if table_name != "patients":
        return dataframe

    if "full_name" in dataframe.columns:
        dataframe = dataframe.withColumn(
            "full_name",
            F.when(F.col("full_name").isNull(), F.lit(None).cast("string")).otherwise(
                F.concat(F.substring(F.trim("full_name"), 1, 1), F.lit("***"))
            ),
        )
    if "cpf" in dataframe.columns:
        cpf_digits = F.regexp_replace(F.col("cpf"), r"\D", "")
        dataframe = dataframe.withColumn(
            "cpf",
            F.when(F.col("cpf").isNull(), F.lit(None).cast("string")).otherwise(
                F.concat(F.lit("***.***.***-"), F.substring(cpf_digits, -2, 2))
            ),
        )
    if "email" in dataframe.columns:
        dataframe = dataframe.withColumn(
            "email",
            F.when(F.col("email").isNull(), F.lit(None).cast("string")).otherwise(
                F.concat(
                    F.substring(F.lower(F.trim("email")), 1, 1),
                    F.lit("***@"),
                    F.regexp_extract(F.lower(F.trim("email")), r"@(.+)$", 1),
                )
            ),
        )
    if "phone" in dataframe.columns:
        phone_digits = F.regexp_replace(F.col("phone"), r"\D", "")
        dataframe = dataframe.withColumn(
            "phone",
            F.when(F.col("phone").isNull(), F.lit(None).cast("string")).otherwise(
                F.concat(F.lit("***-"), F.substring(phone_digits, -4, 4))
            ),
        )
    return dataframe


def ensure_metrics_table(catalog: str):
    table_name = f"{catalog}.observability.dq_run_metrics"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          checked_at TIMESTAMP,
          dq_run_id STRING,
          dq_stage STRING,
          source_table STRING,
          quarantine_table STRING,
          input_rows BIGINT,
          valid_rows BIGINT,
          quarantined_rows BIGINT,
          status STRING,
          violation_summary STRING,
          odate DATE,
          checked_rows BIGINT
        ) USING DELTA
        COMMENT 'Metrics emitted by HealthLake DQX quality gates'
        """
    )

    existing_columns = set(spark.table(table_name).columns)
    missing_columns = []
    if "odate" not in existing_columns:
        missing_columns.append("odate DATE")
    if "checked_rows" not in existing_columns:
        missing_columns.append("checked_rows BIGINT")
    if missing_columns:
        spark.sql(
            f"ALTER TABLE {table_name} ADD COLUMNS ({', '.join(missing_columns)})"
        )


def ensure_promotion_control(catalog: str):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.observability.dq_promotion_control (
          dq_stage STRING NOT NULL,
          odate DATE NOT NULL,
          dq_run_id STRING NOT NULL,
          approved_at TIMESTAMP NOT NULL
        ) USING DELTA
        COMMENT 'Historical approved partitions per DQ stage; written only after every table passes'
        """
    )


METRICS_SCHEMA = StructType(
    [
        StructField("checked_at", TimestampType(), False),
        StructField("dq_run_id", StringType(), False),
        StructField("dq_stage", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("quarantine_table", StringType(), False),
        StructField("input_rows", LongType(), False),
        StructField("valid_rows", LongType(), False),
        StructField("quarantined_rows", LongType(), False),
        StructField("status", StringType(), False),
        StructField("violation_summary", StringType(), False),
        StructField("odate", DateType(), False),
        StructField("checked_rows", LongType(), False),
    ]
)


APPROVAL_SCHEMA = StructType(
    [
        StructField("dq_stage", StringType(), False),
        StructField("odate", DateType(), False),
        StructField("dq_run_id", StringType(), False),
        StructField("approved_at", TimestampType(), False),
    ]
)


def record_approval(catalog: str, stage: str, odate: date, run_id: str, at: datetime):
    approval = spark.createDataFrame([(stage, odate, run_id, at)], APPROVAL_SCHEMA)
    approval.createOrReplaceTempView("_healthlake_dq_approval")
    spark.sql(
        f"""
        MERGE INTO {catalog}.observability.dq_promotion_control AS target
        USING _healthlake_dq_approval AS source
          ON target.dq_stage = source.dq_stage
         AND target.odate = source.odate
        WHEN MATCHED THEN UPDATE SET
          target.odate = source.odate,
          target.dq_run_id = source.dq_run_id,
          target.approved_at = source.approved_at
        WHEN NOT MATCHED THEN INSERT (dq_stage, odate, dq_run_id, approved_at)
          VALUES (source.dq_stage, source.odate, source.dq_run_id, source.approved_at)
        """
    )


def run_quality_gate(args):
    log_status(
        "dq_gate",
        "control_tables_initialization_started",
        stage=args.stage,
        catalog=args.catalog,
        odate=args.odate,
    )
    ensure_metrics_table(args.catalog)
    ensure_promotion_control(args.catalog)
    log_status(
        "dq_gate",
        "control_tables_initialization_completed",
        stage=args.stage,
        catalog=args.catalog,
        odate=args.odate,
    )
    dq_engine = DQEngine(WorkspaceClient())
    check_time = datetime.now(timezone.utc)
    metric_rows = []
    failures = []

    for table_name in tables_for(args.stage):
        source_table = f"{args.catalog}.{source_schema(args.stage)}.{table_name}"
        # Each stage has a dedicated typed quarantine table so raw and cleaned
        # contracts never share a Delta schema.
        quarantine_table = f"{args.catalog}.quarantine.{args.stage}_{table_name}"
        log_status(
            "dq_gate",
            "table_started",
            stage=args.stage,
            source_table=source_table,
            quarantine_table=quarantine_table,
            odate=args.odate,
        )
        source_df = spark.read.table(source_table)
        if args.stage == "bronze_to_silver":
            source_df = with_effective_odate(source_df)
        source_for_odate = source_df.where(
            F.col(snapshot_column(args.stage)) == F.lit(args.odate)
        )
        input_rows = source_for_odate.count()
        log_status(
            "dq_gate",
            "source_count_completed",
            stage=args.stage,
            source_table=source_table,
            odate=args.odate,
            input_rows=input_rows,
        )

        if input_rows == 0:
            reason = f"no rows found for odate={args.odate.isoformat()}"
            failures.append(f"{source_table}: {reason}")
            metric_rows.append(
                (
                    check_time,
                    args.run_id,
                    args.stage,
                    source_table,
                    quarantine_table,
                    0,
                    0,
                    0,
                    "FAILED",
                    json.dumps({"reason": reason}),
                    args.odate,
                    0,
                )
            )
            log_status(
                "dq_gate",
                "table_failed",
                stage=args.stage,
                source_table=source_table,
                odate=args.odate,
                reason=reason,
                input_rows=0,
                checked_rows=0,
                valid_rows=0,
                quarantined_rows=0,
            )
            continue

        # Spark Connect serverless does not support cache/persist. Keep this
        # lazy and derive valid_rows from the exclusive DQX error split.
        checked_df = prepare_for_checks(source_for_odate, args.stage, table_name)
        checked_rows = checked_df.count()
        removed_by_cleaning = input_rows - checked_rows
        log_status(
            "dq_gate",
            "cleaning_completed",
            stage=args.stage,
            source_table=source_table,
            odate=args.odate,
            input_rows=input_rows,
            checked_rows=checked_rows,
            removed_by_cleaning=removed_by_cleaning,
        )
        if checked_rows == 0:
            reason = "cleaning produced an empty snapshot"
            failures.append(f"{source_table}: {reason}")
            metric_rows.append(
                (
                    check_time,
                    args.run_id,
                    args.stage,
                    source_table,
                    quarantine_table,
                    input_rows,
                    0,
                    0,
                    "FAILED",
                    json.dumps(
                        {
                            "reason": reason,
                            "removed_by_cleaning": removed_by_cleaning,
                        }
                    ),
                    args.odate,
                    checked_rows,
                )
            )
            log_status(
                "dq_gate",
                "table_failed",
                stage=args.stage,
                source_table=source_table,
                odate=args.odate,
                reason=reason,
                input_rows=input_rows,
                checked_rows=checked_rows,
                valid_rows=0,
                quarantined_rows=0,
            )
            continue

        _valid_df, invalid_df = dq_engine.apply_checks_and_split(
            checked_df, rules_for(args.stage, table_name)
        )
        quarantined_rows = invalid_df.count()
        # All rules are critical errors, so the two splits are exclusive.
        # Deriving the valid count avoids a second full DQ Spark action.
        valid_rows = checked_rows - quarantined_rows
        status = "FAILED" if quarantined_rows else "PASSED"
        log_status(
            "dq_gate",
            "checks_completed",
            stage=args.stage,
            source_table=source_table,
            odate=args.odate,
            status=status,
            input_rows=input_rows,
            checked_rows=checked_rows,
            valid_rows=valid_rows,
            quarantined_rows=quarantined_rows,
        )

        if quarantined_rows:
            log_status(
                "dq_gate",
                "quarantine_write_started",
                stage=args.stage,
                source_table=source_table,
                quarantine_table=quarantine_table,
                odate=args.odate,
                quarantined_rows=quarantined_rows,
            )
            (
                mask_sensitive_columns(invalid_df, table_name)
                .withColumn("_dq_stage", F.lit(args.stage))
                .withColumn("_dq_run_id", F.lit(args.run_id))
                .withColumn("_dq_odate", F.lit(args.odate))
                .withColumn("_dq_checked_at", F.lit(check_time).cast("timestamp"))
                .withColumn("_dq_source_table", F.lit(source_table))
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(quarantine_table)
            )
            log_status(
                "dq_gate",
                "quarantine_write_completed",
                stage=args.stage,
                source_table=source_table,
                quarantine_table=quarantine_table,
                odate=args.odate,
                quarantined_rows=quarantined_rows,
            )
            failures.append(f"{source_table}: {quarantined_rows} row(s)")

        metric_rows.append(
            (
                check_time,
                args.run_id,
                args.stage,
                source_table,
                quarantine_table,
                input_rows,
                valid_rows,
                quarantined_rows,
                status,
                json.dumps(
                    {
                        "quarantined_rows": quarantined_rows,
                        "removed_by_cleaning": removed_by_cleaning,
                    }
                ),
                args.odate,
                checked_rows,
            )
        )
        log_status(
            "dq_gate",
            "table_completed",
            stage=args.stage,
            source_table=source_table,
            odate=args.odate,
            status=status,
        )

    metrics_table = f"{args.catalog}.observability.dq_run_metrics"
    log_status(
        "dq_gate",
        "metrics_write_started",
        stage=args.stage,
        table=metrics_table,
        odate=args.odate,
        metric_count=len(metric_rows),
    )
    (
        spark.createDataFrame(metric_rows, METRICS_SCHEMA)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(metrics_table)
    )
    log_status(
        "dq_gate",
        "metrics_write_completed",
        stage=args.stage,
        table=metrics_table,
        odate=args.odate,
        metric_count=len(metric_rows),
    )

    if failures:
        log_status(
            "dq_gate",
            "promotion_rejected",
            stage=args.stage,
            odate=args.odate,
            failed_table_count=len(failures),
        )
        raise RuntimeError(
            f"DQX gate {args.stage} for odate={args.odate.isoformat()} failed. "
            "The target layer was not written. "
            + "; ".join(failures)
        )

    # This is the only promotion signal. The valid split is intentionally never
    # written, so all five target tables are refreshed together or not at all.
    log_status(
        "dq_gate",
        "approval_write_started",
        stage=args.stage,
        odate=args.odate,
        run_id=args.run_id,
    )
    record_approval(args.catalog, args.stage, args.odate, args.run_id, check_time)
    log_status(
        "dq_gate",
        "approval_write_completed",
        stage=args.stage,
        odate=args.odate,
        run_id=args.run_id,
    )


def main():
    args = parse_args()
    log_status(
        "dq_gate",
        "task_started",
        stage=args.stage,
        catalog=args.catalog,
        odate=args.odate,
        run_id=args.run_id,
        table_count=len(tables_for(args.stage)),
    )
    try:
        run_quality_gate(args)
    except Exception as error:
        log_status(
            "dq_gate",
            "task_failed",
            stage=args.stage,
            catalog=args.catalog,
            odate=args.odate,
            run_id=args.run_id,
            error_type=type(error).__name__,
            error=str(error),
        )
        raise
    log_status(
        "dq_gate",
        "task_completed",
        stage=args.stage,
        catalog=args.catalog,
        odate=args.odate,
        run_id=args.run_id,
        processed_tables=len(tables_for(args.stage)),
    )


if __name__ == "__main__":
    main()
